/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.Fetcher.hasUsableOffsetForLeaderEpochVersion;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seekValidated(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * Thread Safety: this class is thread-safe.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /* the type of subscription */
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private Pattern subscribedPattern;

    /* the list of topics the user has requested */
    private Set<String> subscription;

    /* The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance. */
    private Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* User-provided listener to be invoked when assignment changes */
    private ConsumerRebalanceListener rebalanceListener;

    private int assignmentId = 0;
    
    private final Lock lock = new ReentrantLock();

    @Override
    public String toString() {
        lock.lock();
        try {
            return "SubscriptionState{" +
                "type=" + subscriptionType +
                ", subscribedPattern=" + subscribedPattern +
                ", subscription=" + String.join(",", subscription) +
                ", groupSubscription=" + String.join(",", groupSubscription) +
                ", defaultResetStrategy=" + defaultResetStrategy +
                ", assignment=" + assignment.partitionStateValues() + " (id=" + assignmentId + ")}";
        } finally {
            lock.unlock();
        }
    }

    public String prettyString() {
        lock.lock();
        try {
            switch (subscriptionType) {
                case NONE:
                    return "None";
                case AUTO_TOPICS:
                    return "Subscribe(" + String.join(",", subscription) + ")";
                case AUTO_PATTERN:
                    return "Subscribe(" + subscribedPattern + ")";
                case USER_ASSIGNED:
                    return "Assign(" + assignedPartitions() + " , id=" + assignmentId + ")";
                default:
                    throw new IllegalStateException("Unrecognized subscription type: " + subscriptionType);
            }
        } finally {
            lock.unlock();
        }
    }

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = new HashSet<>();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * Monotonically increasing id which is incremented after every assignment change. This can
     * be used to check when an assignment has changed.
     *
     * @return The current assignment Id
     */
    int assignmentId() {
        lock.lock();
        try {
            return assignmentId;
        } finally {
            lock.unlock();
        }
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        lock.lock();
        try {
            registerRebalanceListener(listener);
            setSubscriptionType(SubscriptionType.AUTO_TOPICS);
            return changeSubscription(topics);
        } finally {
            lock.unlock();
        }
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        lock.lock();
        try {
            registerRebalanceListener(listener);
            setSubscriptionType(SubscriptionType.AUTO_PATTERN);
            this.subscribedPattern = pattern;
        } finally {
            lock.unlock();
        }
    }

    public boolean subscribeFromPattern(Set<String> topics) {
        lock.lock();
        try {
            if (subscriptionType != SubscriptionType.AUTO_PATTERN)
                throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);
            
            return changeSubscription(topics);
        } finally {
            lock.unlock();
        }
    }

    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        subscription = topicsToSubscribe;
        return true;
    }

    /**
     * Set the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     *
     * @param topics All topics from the group subscription
     * @return true if the group subscription contains topics which are not part of the local subscription
     */
    boolean groupSubscribe(Collection<String> topics) {
        lock.lock();
        try {
            if (!hasAutoAssignedPartitions())
                throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
            groupSubscription = new HashSet<>(topics);
            return !subscription.containsAll(groupSubscription);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    void resetGroupSubscription() {
        lock.lock();
        try {
            groupSubscription = Collections.emptySet();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public boolean assignFromUser(Set<TopicPartition> partitions) {
        lock.lock();
        try {
            setSubscriptionType(SubscriptionType.USER_ASSIGNED);
            
            if (this.assignment.partitionSet().equals(partitions))
                return false;
            
            assignmentId++;
            
            // update the subscribed topics
            Set<String> manualSubscribedTopics = new HashSet<>();
            Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
            for (TopicPartition partition : partitions) {
                TopicPartitionState state = assignment.stateValue(partition);
                if (state == null)
                    state = new TopicPartitionState();
                partitionToState.put(partition, state);
                
                manualSubscribedTopics.add(partition.topic());
            }
            
            this.assignment.set(partitionToState);
            return changeSubscription(manualSubscribedTopics);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return true if assignments matches subscription, otherwise false
     */
    public boolean checkAssignmentMatchedSubscription(Collection<TopicPartition> assignments) {
        lock.lock();
        try {
            for (TopicPartition topicPartition : assignments) {
                if (this.subscribedPattern != null) {
                    if (!this.subscribedPattern.matcher(topicPartition.topic()).matches()) {
                        log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                            topicPartition,
                            this.subscribedPattern);
                        
                        return false;
                    }
                } else {
                    if (!this.subscription.contains(topicPartition.topic())) {
                        log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);
                        
                        return false;
                    }
                }
            }
            
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     */
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        lock.lock();
        try {
            if (!this.hasAutoAssignedPartitions())
                throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");
            
            Map<TopicPartition, TopicPartitionState> assignedPartitionStates = new HashMap<>(assignments.size());
            for (TopicPartition tp : assignments) {
                TopicPartitionState state = this.assignment.stateValue(tp);
                if (state == null)
                    state = new TopicPartitionState();
                assignedPartitionStates.put(tp, state);
            }
            
            assignmentId++;
            this.assignment.set(assignedPartitionStates);
        } finally {
            lock.unlock();
        }
    }

    private void registerRebalanceListener(ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        this.rebalanceListener = listener;
    }

    /**
     * Check whether pattern subscription is in use.
     *
     */
    boolean hasPatternSubscription() {
        lock.lock();
        try {
            return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
        } finally {
            lock.unlock();
        }
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        lock.lock();
        try {
            return this.subscriptionType == SubscriptionType.NONE;
        } finally {
            lock.unlock();
        }
    }

    public void unsubscribe() {
        lock.lock();
        try {
            this.subscription = Collections.emptySet();
            this.groupSubscription = Collections.emptySet();
            this.assignment.clear();
            this.subscribedPattern = null;
            this.subscriptionType = SubscriptionType.NONE;
            this.assignmentId++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    boolean matchesSubscribedPattern(String topic) {
        lock.lock();
        try {
            Pattern pattern = this.subscribedPattern;
            if (hasPatternSubscription() && pattern != null)
                return pattern.matcher(topic).matches();
            return false;
        } finally {
            lock.unlock();
        }
    }

    public Set<String> subscription() {
        lock.lock();
        try {
            if (hasAutoAssignedPartitions())
                return this.subscription;
            return Collections.emptySet();
        } finally {
            lock.unlock();
        }
    }

    public Set<TopicPartition> pausedPartitions() {
        lock.lock();
        try {
            return collectPartitions(TopicPartitionState::isPaused, Collectors.toSet());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the subscription topics for which metadata is required. For the leader, this will include
     * the union of the subscriptions of all group members. For followers, it is just that member's
     * subscription. This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    Set<String> metadataTopics() {
        lock.lock();
        try {
            if (groupSubscription.isEmpty())
                return subscription;
            else if (groupSubscription.containsAll(subscription))
                return groupSubscription;
            else {
                // When subscription changes `groupSubscription` may be outdated, ensure that
                // new subscription topics are returned.
                Set<String> topics = new HashSet<>(groupSubscription);
                topics.addAll(subscription);
                return topics;
            }
        } finally {
            lock.unlock();
        }
    }

    boolean needsMetadata(String topic) {
        lock.lock();
        try {
            return subscription.contains(topic) || groupSubscription.contains(topic);
        } finally {
            lock.unlock();
        }
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    private TopicPartitionState assignedStateOrNull(TopicPartition tp) {
        return this.assignment.stateValue(tp);
    }

    public void seekValidated(TopicPartition tp, FetchPosition position) {
        lock.lock();
        try {
            assignedState(tp).seekValidated(position);
        } finally {
            lock.unlock();
        }
    }

    public void seek(TopicPartition tp, long offset) {
        seekValidated(tp, new FetchPosition(offset));
    }

    public void seekUnvalidated(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seekUnvalidated(position);
    }

    void maybeSeekUnvalidated(TopicPartition tp, long offset, OffsetResetStrategy requestedResetStrategy) {
        lock.lock();
        try {
            TopicPartitionState state = assignedStateOrNull(tp);
            if (state == null) {
                log.debug("Skipping reset of partition {} since it is no longer assigned", tp);
            } else if (!state.awaitingReset()) {
                log.debug("Skipping reset of partition {} since reset is no longer needed", tp);
            } else if (requestedResetStrategy != state.resetStrategy) {
                log.debug("Skipping reset of partition {} since an alternative reset has been requested", tp);
            } else {
                log.info("Resetting offset for partition {} to offset {}.", tp, offset);
                state.seekUnvalidated(new FetchPosition(offset));
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions
     */
    public Set<TopicPartition> assignedPartitions() {
        lock.lock();
        try {
            return new HashSet<>(this.assignment.partitionSet());
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return a modifiable copy of the currently assigned partitions as a list
     */
    public List<TopicPartition> assignedPartitionsList() {
        lock.lock();
        try {
            return new ArrayList<>(this.assignment.partitionSet());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    int numAssignedPartitions() {
        lock.lock();
        try {
            return this.assignment.size();
        } finally {
            lock.unlock();
        }
    }

    List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        lock.lock();
        try {
            return assignment.stream()
                .filter(tpState -> isAvailable.test(tpState.topicPartition()) && tpState.value().isFetchable())
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasAutoAssignedPartitions() {
        lock.lock();
        try {
            return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
        } finally {
            lock.unlock();
        }
    }

    public void position(TopicPartition tp, FetchPosition position) {
        lock.lock();
        try {
            assignedState(tp).position(position);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enter the offset validation state if the leader for this partition is known to support a usable version of the
     * OffsetsForLeaderEpoch API. If the leader node does not support the API, simply complete the offset validation.
     *
     * @param apiVersions supported API versions
     * @param tp topic partition to validate
     * @param leaderAndEpoch leader epoch of the topic partition
     * @return true if we enter the offset validation state
     */
    public boolean maybeValidatePositionForCurrentLeader(ApiVersions apiVersions,
                                                                      TopicPartition tp,
                                                                      Metadata.LeaderAndEpoch leaderAndEpoch) {
        lock.lock();
        try {
            if (leaderAndEpoch.leader.isPresent()) {
                NodeApiVersions nodeApiVersions = apiVersions.get(leaderAndEpoch.leader.get().idString());
                if (nodeApiVersions == null || hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                    return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
                } else {
                    // If the broker does not support a newer version of OffsetsForLeaderEpoch, we skip validation
                    assignedState(tp).updatePositionLeaderNoValidation(leaderAndEpoch);
                    return false;
                }
            } else {
                return assignedState(tp).maybeValidatePosition(leaderAndEpoch);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attempt to complete validation with the end offset returned from the OffsetForLeaderEpoch request.
     * @return Log truncation details if detected and no reset policy is defined.
     */
    public Optional<LogTruncation> maybeCompleteValidation(TopicPartition tp,
                                                                        FetchPosition requestPosition,
                                                                        EpochEndOffset epochEndOffset) {
        lock.lock();
        try {
            TopicPartitionState state = assignedStateOrNull(tp);
            if (state == null) {
                log.debug("Skipping completed validation for partition {} which is not currently assigned.", tp);
            } else if (!state.awaitingValidation()) {
                log.debug("Skipping completed validation for partition {} which is no longer expecting validation.", tp);
            } else {
                SubscriptionState.FetchPosition currentPosition = state.position;
                if (!currentPosition.equals(requestPosition)) {
                    log.debug("Skipping completed validation for partition {} since the current position {} " +
                        "no longer matches the position {} when the request was sent",
                        tp, currentPosition, requestPosition);
                } else if (epochEndOffset.hasUndefinedEpochOrOffset()) {
                    if (hasDefaultOffsetResetPolicy()) {
                        log.info("Truncation detected for partition {} at offset {}, resetting offset",
                            tp, currentPosition);
                        
                        requestOffsetReset(tp);
                    } else {
                        log.warn("Truncation detected for partition {} at offset {}, but no reset policy is set",
                            tp, currentPosition);
                        return Optional.of(new LogTruncation(tp, requestPosition, Optional.empty()));
                    }
                } else if (epochEndOffset.endOffset() < currentPosition.offset) {
                    if (hasDefaultOffsetResetPolicy()) {
                        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                            epochEndOffset.endOffset(), Optional.of(epochEndOffset.leaderEpoch()),
                            currentPosition.currentLeader);
                        log.info("Truncation detected for partition {} at offset {}, resetting offset to " +
                            "the first offset known to diverge {}", tp, currentPosition, newPosition);
                        state.seekValidated(newPosition);
                    } else {
                        OffsetAndMetadata divergentOffset = new OffsetAndMetadata(epochEndOffset.endOffset(),
                            Optional.of(epochEndOffset.leaderEpoch()), null);
                        log.warn("Truncation detected for partition {} at offset {} (the end offset from the " +
                            "broker is {}), but no reset policy is set",
                            tp, currentPosition, divergentOffset);
                        return Optional.of(new LogTruncation(tp, requestPosition, Optional.of(divergentOffset)));
                    }
                } else {
                    state.completeValidation();
                }
            }
            
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    public boolean awaitingValidation(TopicPartition tp) {
        lock.lock();
        try {
            return assignedState(tp).awaitingValidation();
        } finally {
            lock.unlock();
        }
    }

    public void completeValidation(TopicPartition tp) {
        lock.lock();
        try {
            assignedState(tp).completeValidation();
        } finally {
            lock.unlock();
        }
    }

    public FetchPosition validPosition(TopicPartition tp) {
        lock.lock();
        try {
            return assignedState(tp).validPosition();
        } finally {
            lock.unlock();
        }
    }

    public FetchPosition position(TopicPartition tp) {
        lock.lock();
        try {
            return assignedState(tp).position;
        } finally {
            lock.unlock();
        }
    }

    Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        lock.lock();
        try {
            TopicPartitionState topicPartitionState = assignedState(tp);
            if (isolationLevel == IsolationLevel.READ_COMMITTED)
                return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
            else
                return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
        } finally {
            lock.unlock();
        }
    }

    Long partitionLead(TopicPartition tp) {
        lock.lock();
        try {
            TopicPartitionState topicPartitionState = assignedState(tp);
            return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
        } finally {
            lock.unlock();
        }
    }

    void updateHighWatermark(TopicPartition tp, long highWatermark) {
        lock.lock();
        try {
            assignedState(tp).highWatermark(highWatermark);
        } finally {
            lock.unlock();
        }
    }

    void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        lock.lock();
        try {
            assignedState(tp).logStartOffset(logStartOffset);
        } finally {
            lock.unlock();
        }
    }

    void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        lock.lock();
        try {
            assignedState(tp).lastStableOffset(lastStableOffset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set the preferred read replica with a lease timeout. After this time, the replica will no longer be valid and
     * {@link #preferredReadReplica(TopicPartition, long)} will return an empty result.
     *
     * @param tp The topic partition
     * @param preferredReadReplicaId The preferred read replica
     * @param timeMs The time at which this preferred replica is no longer valid
     */
    public void updatePreferredReadReplica(TopicPartition tp, int preferredReadReplicaId, LongSupplier timeMs) {
        lock.lock();
        try {
            assignedState(tp).updatePreferredReadReplica(preferredReadReplicaId, timeMs);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the preferred read replica
     *
     * @param tp The topic partition
     * @param timeMs The current time
     * @return Returns the current preferred read replica, if it has been set and if it has not expired.
     */
    public Optional<Integer> preferredReadReplica(TopicPartition tp, long timeMs) {
        lock.lock();
        try {
            final TopicPartitionState topicPartitionState = assignedStateOrNull(tp);
            if (topicPartitionState == null) {
                return Optional.empty();
            } else {
                return topicPartitionState.preferredReadReplica(timeMs);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unset the preferred read replica. This causes the fetcher to go back to the leader for fetches.
     *
     * @param tp The topic partition
     * @return true if the preferred read replica was set, false otherwise.
     */
    public Optional<Integer> clearPreferredReadReplica(TopicPartition tp) {
        lock.lock();
        try {
            return assignedState(tp).clearPreferredReadReplica();
        } finally {
            lock.unlock();
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        lock.lock();
        try {
            Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
            assignment.stream().forEach(state -> {
                TopicPartitionState partitionState = state.value();
                if (partitionState.hasValidPosition())
                    allConsumed.put(state.topicPartition(), new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.offsetEpoch, ""));
            });
            return allConsumed;
        } finally {
            lock.unlock();
        }
    }

    public void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        lock.lock();
        try {
            assignedState(partition).reset(offsetResetStrategy);
        } finally {
            lock.unlock();
        }
    }

    public void requestOffsetReset(Collection<TopicPartition> partitions, OffsetResetStrategy offsetResetStrategy) {
        lock.lock();
        try {
            partitions.forEach(tp -> {
                log.info("Seeking to {} offset of partition {}", offsetResetStrategy, tp);
                assignedState(tp).reset(offsetResetStrategy);
            });
        } finally {
            lock.unlock();
        }
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    void setNextAllowedRetry(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        lock.lock();
        try {
            for (TopicPartition partition : partitions) {
                assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
            }
        } finally {
            lock.unlock();
        }
    }

    boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        lock.lock();
        try {
            return assignedState(partition).awaitingReset();
        } finally {
            lock.unlock();
        }
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        lock.lock();
        try {
            return assignedState(partition).resetStrategy();
        } finally {
            lock.unlock();
        }
    }

    public boolean hasAllFetchPositions() {
        lock.lock();
        try {
            return assignment.stream().allMatch(state -> state.value().hasValidPosition());
        } finally {
            lock.unlock();
        }
    }

    public Set<TopicPartition> initializingPartitions() {
        lock.lock();
        try {
            return collectPartitions(state -> state.fetchState.equals(FetchStates.INITIALIZING), Collectors.toSet());
        } finally {
            lock.unlock();
        }
    }

    private <T extends Collection<TopicPartition>> T collectPartitions(Predicate<TopicPartitionState> filter, Collector<TopicPartition, ?, T> collector) {
        return assignment.stream()
                .filter(state -> filter.test(state.value()))
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(collector);
    }


    public void resetInitializingPositions() {
        lock.lock();
        try {
            final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
            assignment.stream().forEach(state -> {
                TopicPartition tp = state.topicPartition();
                TopicPartitionState partitionState = state.value();
                if (partitionState.fetchState.equals(FetchStates.INITIALIZING)) {
                    if (defaultResetStrategy == OffsetResetStrategy.NONE)
                        partitionsWithNoOffsets.add(tp);
                    else
                        requestOffsetReset(tp);
                }
            });
            
            if (!partitionsWithNoOffsets.isEmpty())
                throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
        } finally {
            lock.unlock();
        }
    }

    public Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        lock.lock();
        try {
            return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
        } finally {
            lock.unlock();
        }
    }

    public Set<TopicPartition> partitionsNeedingValidation(long nowMs) {
        lock.lock();
        try {
            return collectPartitions(state -> state.awaitingValidation() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
        } finally {
            lock.unlock();
        }
    }

    public boolean isAssigned(TopicPartition tp) {
        lock.lock();
        try {
            return assignment.contains(tp);
        } finally {
            lock.unlock();
        }
    }

    public boolean isPaused(TopicPartition tp) {
        lock.lock();
        try {
            TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
            return assignedOrNull != null && assignedOrNull.isPaused();
        } finally {
            lock.unlock();
        }
    }

    boolean isFetchable(TopicPartition tp) {
        lock.lock();
        try {
            TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
            return assignedOrNull != null && assignedOrNull.isFetchable();
        } finally {
            lock.unlock();
        }
    }

    public boolean hasValidPosition(TopicPartition tp) {
        lock.lock();
        try {
            TopicPartitionState assignedOrNull = assignedStateOrNull(tp);
            return assignedOrNull != null && assignedOrNull.hasValidPosition();
        } finally {
            lock.unlock();
        }
    }

    public void pause(TopicPartition tp) {
        lock.lock();
        try {
            assignedState(tp).pause();
        } finally {
            lock.unlock();
        }
    }

    public void resume(TopicPartition tp) {
        lock.lock();
        try {
            assignedState(tp).resume();
        } finally {
            lock.unlock();
        }
    }

    void requestFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        lock.lock();
        try {
            for (TopicPartition partition : partitions) {
                // by the time the request failed, the assignment may no longer
                // contain this partition any more, in which case we would just ignore.
                final TopicPartitionState state = assignedStateOrNull(partition);
                if (state != null)
                    state.requestFailed(nextRetryTimeMs);
            }
        } finally {
            lock.unlock();
        }
    }

    void movePartitionToEnd(TopicPartition tp) {
        lock.lock();
        try {
            assignment.moveToEnd(tp);
        } finally {
            lock.unlock();
        }
    }

    public ConsumerRebalanceListener rebalanceListener() {
        lock.lock();
        try {
            return rebalanceListener;
        } finally {
            lock.unlock();
        }
    }

    private static class TopicPartitionState {

        private FetchState fetchState;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;
        private Integer preferredReadReplica;
        private Long preferredReadReplicaExpireTimeMs;

        TopicPartitionState() {
            this.paused = false;
            this.fetchState = FetchStates.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
            this.preferredReadReplica = null;
        }

        private void transitionState(FetchState newState, Runnable runIfTransitioned) {
            FetchState nextState = this.fetchState.transitionTo(newState);
            if (nextState.equals(newState)) {
                this.fetchState = nextState;
                runIfTransitioned.run();
                if (this.position == null && nextState.requiresPosition()) {
                    throw new IllegalStateException("Transitioned subscription state to " + nextState + ", but position is null");
                } else if (!nextState.requiresPosition()) {
                    this.position = null;
                }
            }
        }

        private Optional<Integer> preferredReadReplica(long timeMs) {
            if (preferredReadReplicaExpireTimeMs != null && timeMs > preferredReadReplicaExpireTimeMs) {
                preferredReadReplica = null;
                return Optional.empty();
            } else {
                return Optional.ofNullable(preferredReadReplica);
            }
        }

        private void updatePreferredReadReplica(int preferredReadReplica, LongSupplier timeMs) {
            if (this.preferredReadReplica == null || preferredReadReplica != this.preferredReadReplica) {
                this.preferredReadReplica = preferredReadReplica;
                this.preferredReadReplicaExpireTimeMs = timeMs.getAsLong();
            }
        }

        private Optional<Integer> clearPreferredReadReplica() {
            if (preferredReadReplica != null) {
                int removedReplicaId = this.preferredReadReplica;
                this.preferredReadReplica = null;
                this.preferredReadReplicaExpireTimeMs = null;
                return Optional.of(removedReplicaId);
            } else {
                return Optional.empty();
            }
        }

        private void reset(OffsetResetStrategy strategy) {
            transitionState(FetchStates.AWAIT_RESET, () -> {
                this.resetStrategy = strategy;
                this.nextRetryTimeMs = null;
            });
        }

        /**
         * Check if the position exists and needs to be validated. If so, enter the AWAIT_VALIDATION state. This method
         * also will update the position with the current leader and epoch.
         *
         * @param currentLeaderAndEpoch leader and epoch to compare the offset with
         * @return true if the position is now awaiting validation
         */
        private boolean maybeValidatePosition(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (this.fetchState.equals(FetchStates.AWAIT_RESET)) {
                return false;
            }

            if (!currentLeaderAndEpoch.leader.isPresent()) {
                return false;
            }

            if (position != null && !position.currentLeader.equals(currentLeaderAndEpoch)) {
                FetchPosition newPosition = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                validatePosition(newPosition);
                preferredReadReplica = null;
            }
            return this.fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        /**
         * For older versions of the API, we cannot perform offset validation so we simply transition directly to FETCHING
         */
        private void updatePositionLeaderNoValidation(Metadata.LeaderAndEpoch currentLeaderAndEpoch) {
            if (position != null) {
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = new FetchPosition(position.offset, position.offsetEpoch, currentLeaderAndEpoch);
                    this.nextRetryTimeMs = null;
                });
            }
        }

        private void validatePosition(FetchPosition position) {
            if (position.offsetEpoch.isPresent() && position.currentLeader.epoch.isPresent()) {
                transitionState(FetchStates.AWAIT_VALIDATION, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            } else {
                // If we have no epoch information for the current position, then we can skip validation
                transitionState(FetchStates.FETCHING, () -> {
                    this.position = position;
                    this.nextRetryTimeMs = null;
                });
            }
        }

        /**
         * Clear the awaiting validation state and enter fetching.
         */
        private void completeValidation() {
            if (hasPosition()) {
                transitionState(FetchStates.FETCHING, () -> this.nextRetryTimeMs = null);
            }
        }

        private boolean awaitingValidation() {
            return fetchState.equals(FetchStates.AWAIT_VALIDATION);
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return fetchState.equals(FetchStates.AWAIT_RESET);
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void requestFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return fetchState.hasValidPosition();
        }

        private boolean hasPosition() {
            return position != null;
        }

        private boolean isPaused() {
            return paused;
        }

        private void seekValidated(FetchPosition position) {
            transitionState(FetchStates.FETCHING, () -> {
                this.position = position;
                this.resetStrategy = null;
                this.nextRetryTimeMs = null;
            });
        }

        private void seekUnvalidated(FetchPosition fetchPosition) {
            seekValidated(fetchPosition);
            validatePosition(fetchPosition);
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private FetchPosition validPosition() {
            if (hasValidPosition()) {
                return position;
            } else {
                return null;
            }
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

        private void highWatermark(Long highWatermark) {
            this.highWatermark = highWatermark;
        }

        private void logStartOffset(Long logStartOffset) {
            this.logStartOffset = logStartOffset;
        }

        private void lastStableOffset(Long lastStableOffset) {
            this.lastStableOffset = lastStableOffset;
        }

        private OffsetResetStrategy resetStrategy() {
            return resetStrategy;
        }
    }

    /**
     * The fetch state of a partition. This class is used to determine valid state transitions and expose the some of
     * the behavior of the current fetch state. Actual state variables are stored in the {@link TopicPartitionState}.
     */
    interface FetchState {
        default FetchState transitionTo(FetchState newState) {
            if (validTransitions().contains(newState)) {
                return newState;
            } else {
                return this;
            }
        }

        /**
         * Return the valid states which this state can transition to
         */
        Collection<FetchState> validTransitions();

        /**
         * Test if this state requires a position to be set
         */
        boolean requiresPosition();

        /**
         * Test if this state is considered to have a valid position which can be used for fetching
         */
        boolean hasValidPosition();
    }

    /**
     * An enumeration of all the possible fetch states. The state transitions are encoded in the values returned by
     * {@link FetchState#validTransitions}.
     */
    enum FetchStates implements FetchState {
        INITIALIZING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        FETCHING() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return true;
            }
        },

        AWAIT_RESET() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET);
            }

            @Override
            public boolean requiresPosition() {
                return false;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        },

        AWAIT_VALIDATION() {
            @Override
            public Collection<FetchState> validTransitions() {
                return Arrays.asList(FetchStates.FETCHING, FetchStates.AWAIT_RESET, FetchStates.AWAIT_VALIDATION);
            }

            @Override
            public boolean requiresPosition() {
                return true;
            }

            @Override
            public boolean hasValidPosition() {
                return false;
            }
        }
    }

    /**
     * Represents the position of a partition subscription.
     *
     * This includes the offset and epoch from the last record in
     * the batch from a FetchResponse. It also includes the leader epoch at the time the batch was consumed.
     */
    public static class FetchPosition {
        public final long offset;
        final Optional<Integer> offsetEpoch;
        final Metadata.LeaderAndEpoch currentLeader;

        FetchPosition(long offset) {
            this(offset, Optional.empty(), Metadata.LeaderAndEpoch.noLeaderOrEpoch());
        }

        public FetchPosition(long offset, Optional<Integer> offsetEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.offsetEpoch = Objects.requireNonNull(offsetEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchPosition that = (FetchPosition) o;
            return offset == that.offset &&
                    offsetEpoch.equals(that.offsetEpoch) &&
                    currentLeader.equals(that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, offsetEpoch, currentLeader);
        }

        @Override
        public String toString() {
            return "FetchPosition{" +
                    "offset=" + offset +
                    ", offsetEpoch=" + offsetEpoch +
                    ", currentLeader=" + currentLeader +
                    '}';
        }
    }

    public static class LogTruncation {
        public final TopicPartition topicPartition;
        public final FetchPosition fetchPosition;
        public final Optional<OffsetAndMetadata> divergentOffsetOpt;

        public LogTruncation(TopicPartition topicPartition,
                             FetchPosition fetchPosition,
                             Optional<OffsetAndMetadata> divergentOffsetOpt) {
            this.topicPartition = topicPartition;
            this.fetchPosition = fetchPosition;
            this.divergentOffsetOpt = divergentOffsetOpt;
        }

        @Override
        public String toString() {
            StringBuilder bldr = new StringBuilder()
                .append("(partition=")
                .append(topicPartition)
                .append(", fetchOffset=")
                .append(fetchPosition.offset)
                .append(", fetchEpoch=")
                .append(fetchPosition.offsetEpoch);

            if (divergentOffsetOpt.isPresent()) {
                OffsetAndMetadata divergentOffset = divergentOffsetOpt.get();
                bldr.append(", divergentOffset=")
                    .append(divergentOffset.offset())
                    .append(", divergentEpoch=")
                    .append(divergentOffset.leaderEpoch());
            } else {
                bldr.append(", divergentOffset=unknown")
                    .append(", divergentEpoch=unknown");
            }

            return bldr.append(")").toString();

        }
    }
}
