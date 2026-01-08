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
package org.apache.kafka.clients.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;


/**
 * A mock of the {@link Consumer} interface you can use for testing code that uses Kafka. This class is <i> not
 * threadsafe </i>. However, you can use the {@link #schedulePollTask(Runnable)} method to write multithreaded tests
 * where a driver thread waits for {@link #poll(Duration)} to be called by a background thread and then can safely perform
 * operations during a callback.
 */
public class MockConsumer<K, V> implements Consumer<K, V> {

    private final Map<String, List<PartitionInfo>> partitions;
    private final SubscriptionState subscriptions;
    private final Map<TopicPartition, Long> beginningOffsets;
    private final Map<TopicPartition, Long> endOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> committed;
    private final Queue<Runnable> pollTasks;
    private final Set<TopicPartition> paused;

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private KafkaException pollException;
    private KafkaException offsetsException;
    private AtomicBoolean wakeup;
    private Duration lastPollTimeout;
    private boolean closed;
    private boolean shouldRebalance;
    
    private final Lock lock = new ReentrantLock();
    private final Lock pollLock = new ReentrantLock();

    public MockConsumer(OffsetResetStrategy offsetResetStrategy) {
        this.subscriptions = new SubscriptionState(new LogContext(), offsetResetStrategy);
        this.partitions = new HashMap<>();
        this.records = new HashMap<>();
        this.paused = new HashSet<>();
        this.closed = false;
        this.beginningOffsets = new HashMap<>();
        this.endOffsets = new HashMap<>();
        this.pollTasks = new LinkedList<>();
        this.pollException = null;
        this.wakeup = new AtomicBoolean(false);
        this.committed = new HashMap<>();
        this.shouldRebalance = false;
    }

    @Override
    public Set<TopicPartition> assignment() {
        lock.lock();
        try {
            return this.subscriptions.assignedPartitions();
        } finally {
            lock.unlock();
        }
    }

    /** Simulate a rebalance event. */
    public void rebalance(Collection<TopicPartition> newAssignment) {
        lock.lock();
        try {
            // TODO: Rebalance callbacks
            this.records.clear();
            this.subscriptions.assignFromSubscribed(newAssignment);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<String> subscription() {
        lock.lock();
        try {
            return this.subscriptions.subscription();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        lock.lock();
        try {
            subscribe(topics, new NoOpConsumerRebalanceListener());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
        lock.lock();
        try {
            ensureNotClosed();
            committed.clear();
            this.subscriptions.subscribe(pattern, listener);
            Set<String> topicsToSubscribe = new HashSet<>();
            for (String topic: partitions.keySet()) {
                if (pattern.matcher(topic).matches() &&
                    !subscriptions.subscription().contains(topic))
                    topicsToSubscribe.add(topic);
            }
            ensureNotClosed();
            this.subscriptions.subscribeFromPattern(topicsToSubscribe);
            final Set<TopicPartition> assignedPartitions = new HashSet<>();
            for (final String topic : topicsToSubscribe) {
                for (final PartitionInfo info : this.partitions.get(topic)) {
                    assignedPartitions.add(new TopicPartition(topic, info.partition()));
                }
                
            }
            subscriptions.assignFromSubscribed(assignedPartitions);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribe(Pattern pattern) {
        lock.lock();
        try {
            subscribe(pattern, new NoOpConsumerRebalanceListener());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, final ConsumerRebalanceListener listener) {
        lock.lock();
        try {
            ensureNotClosed();
            committed.clear();
            this.subscriptions.subscribe(new HashSet<>(topics), listener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            ensureNotClosed();
            committed.clear();
            this.subscriptions.assignFromUser(new HashSet<>(partitions));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unsubscribe() {
        lock.lock();
        try {
            ensureNotClosed();
            committed.clear();
            subscriptions.unsubscribe();
        } finally {
            lock.unlock();
        }
    }

    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        lock.lock();
        try {
            return poll(Duration.ofMillis(timeout));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        lock.lock();
        try {
            ensureNotClosed();
            
            lastPollTimeout = timeout;
            
            // Synchronize around the entire execution so new tasks to be triggered on subsequent poll calls can be added in
            // the callback
            pollLock.lock();
            try {
                Runnable task = pollTasks.poll();
                if (task != null)
                    task.run();
            } finally {
                pollLock.unlock();
            }
            
            if (wakeup.get()) {
                wakeup.set(false);
                throw new WakeupException();
            }
            
            if (pollException != null) {
                RuntimeException exception = this.pollException;
                this.pollException = null;
                throw exception;
            }
            
            // Handle seeks that need to wait for a poll() call to be processed
            for (TopicPartition tp : subscriptions.assignedPartitions())
                if (!subscriptions.hasValidPosition(tp))
                    updateFetchPosition(tp);
            
            // update the consumed offset
            final Map<TopicPartition, List<ConsumerRecord<K, V>>> results = new HashMap<>();
            final List<TopicPartition> toClear = new ArrayList<>();
            
            for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : this.records.entrySet()) {
                if (!subscriptions.isPaused(entry.getKey())) {
                    final List<ConsumerRecord<K, V>> recs = entry.getValue();
                    for (final ConsumerRecord<K, V> rec : recs) {
                        long position = subscriptions.position(entry.getKey()).offset;
                        
                        if (beginningOffsets.get(entry.getKey()) != null && beginningOffsets.get(entry.getKey()) > position) {
                            throw new OffsetOutOfRangeException(Collections.singletonMap(entry.getKey(), position));
                        }
                        
                        if (assignment().contains(entry.getKey()) && rec.offset() >= position) {
                            results.computeIfAbsent(entry.getKey(), partition -> new ArrayList<>()).add(rec);
                            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(Optional.empty(), rec.leaderEpoch());
                            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                                rec.offset() + 1, rec.leaderEpoch(), leaderAndEpoch);
                            subscriptions.position(entry.getKey(), newPosition);
                        }
                    }
                    toClear.add(entry.getKey());
                }
            }
            
            toClear.forEach(p -> this.records.remove(p));
            return new ConsumerRecords<>(results);
        } finally {
            lock.unlock();
        }
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        lock.lock();
        try {
            ensureNotClosed();
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            Set<TopicPartition> currentAssigned = this.subscriptions.assignedPartitions();
            if (!currentAssigned.contains(tp))
                throw new IllegalStateException("Cannot add records for a partition that is not assigned to the consumer");
            List<ConsumerRecord<K, V>> recs = this.records.computeIfAbsent(tp, k -> new ArrayList<>());
            recs.add(record);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @deprecated Use {@link #setPollException(KafkaException)} instead
     */
    @Deprecated
    public void setException(KafkaException exception) {
        lock.lock();
        try {
            setPollException(exception);
        } finally {
            lock.unlock();
        }
    }

    public void setPollException(KafkaException exception) {
        lock.lock();
        try {
            this.pollException = exception;
        } finally {
            lock.unlock();
        }
    }

    public void setOffsetsException(KafkaException exception) {
        lock.lock();
        try {
            this.offsetsException = exception;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        lock.lock();
        try {
            ensureNotClosed();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
                committed.put(entry.getKey(), entry.getValue());
            if (callback != null) {
                callback.onComplete(offsets, null);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        lock.lock();
        try {
            commitAsync(offsets, null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitAsync() {
        lock.lock();
        try {
            commitAsync(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        lock.lock();
        try {
            ensureNotClosed();
            commitAsync(this.subscriptions.allConsumed(), callback);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitSync() {
        lock.lock();
        try {
            commitSync(this.subscriptions.allConsumed());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitSync(Duration timeout) {
        lock.lock();
        try {
            commitSync(this.subscriptions.allConsumed());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        commitSync(offsets);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        lock.lock();
        try {
            ensureNotClosed();
            subscriptions.seek(partition, offset);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        ensureNotClosed();
        subscriptions.seek(partition, offsetAndMetadata.offset());
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(final TopicPartition partition) {
        lock.lock();
        try {
            return committed(Collections.singleton(partition)).get(partition);
        } finally {
            lock.unlock();
        }
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(final TopicPartition partition, final Duration timeout) {
        return committed(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        lock.lock();
        try {
            ensureNotClosed();
            
            return partitions.stream()
                .filter(committed::containsKey)
                .collect(Collectors.toMap(tp -> tp, tp -> subscriptions.isAssigned(tp) ?
                    committed.get(tp) : new OffsetAndMetadata(0)));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        lock.lock();
        try {
            return committed(partitions);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        lock.lock();
        try {
            ensureNotClosed();
            if (!this.subscriptions.isAssigned(partition))
                throw new IllegalArgumentException("You can only check the position for partitions assigned to this consumer.");
            SubscriptionState.FetchPosition position = this.subscriptions.position(partition);
            if (position == null) {
                updateFetchPosition(partition);
                position = this.subscriptions.position(partition);
            }
            return position.offset;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        lock.lock();
        try {
            return position(partition);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            ensureNotClosed();
            subscriptions.requestOffsetReset(partitions, OffsetResetStrategy.EARLIEST);
        } finally {
            lock.unlock();
        }
    }

    public void updateBeginningOffsets(Map<TopicPartition, Long> newOffsets) {
        lock.lock();
        try {
            beginningOffsets.putAll(newOffsets);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            ensureNotClosed();
            subscriptions.requestOffsetReset(partitions, OffsetResetStrategy.LATEST);
        } finally {
            lock.unlock();
        }
    }

    public void updateEndOffsets(final Map<TopicPartition, Long> newOffsets) {
        lock.lock();
        try {
            endOffsets.putAll(newOffsets);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        lock.lock();
        try {
            ensureNotClosed();
            return Collections.emptyMap();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        lock.lock();
        try {
            ensureNotClosed();
            return this.partitions.get(topic);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        lock.lock();
        try {
            ensureNotClosed();
            return partitions;
        } finally {
            lock.unlock();
        }
    }

    public void updatePartitions(String topic, List<PartitionInfo> partitions) {
        lock.lock();
        try {
            ensureNotClosed();
            this.partitions.put(topic, partitions);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            for (TopicPartition partition : partitions) {
                subscriptions.pause(partition);
                paused.add(partition);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            for (TopicPartition partition : partitions) {
                subscriptions.resume(partition);
                paused.remove(partition);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        lock.lock();
        try {
            throw new UnsupportedOperationException("Not implemented yet.");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            if (offsetsException != null) {
                RuntimeException exception = this.offsetsException;
                this.offsetsException = null;
                throw exception;
            }
            Map<TopicPartition, Long> result = new HashMap<>();
            for (TopicPartition tp : partitions) {
                Long beginningOffset = beginningOffsets.get(tp);
                if (beginningOffset == null)
                    throw new IllegalStateException("The partition " + tp + " does not have a beginning offset.");
                result.put(tp, beginningOffset);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        lock.lock();
        try {
            if (offsetsException != null) {
                RuntimeException exception = this.offsetsException;
                this.offsetsException = null;
                throw exception;
            }
            Map<TopicPartition, Long> result = new HashMap<>();
            for (TopicPartition tp : partitions) {
                Long endOffset = endOffsets.get(tp);
                if (endOffset == null)
                    throw new IllegalStateException("The partition " + tp + " does not have an end offset.");
                result.put(tp, endOffset);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            close(KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    @Deprecated
    @Override
    public void close(long timeout, TimeUnit unit) {
        lock.lock();
        try {
            this.closed = true;
        } finally {
            lock.unlock();
        }
    }

    public boolean closed() {
        lock.lock();
        try {
            return this.closed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void wakeup() {
        lock.lock();
        try {
            wakeup.set(true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Schedule a task to be executed during a poll(). One enqueued task will be executed per {@link #poll(Duration)}
     * invocation. You can use this repeatedly to mock out multiple responses to poll invocations.
     * @param task the task to be executed
     */
    public void schedulePollTask(Runnable task) {
        lock.lock();
        try {
            pollLock.lock();
            try {
                pollTasks.add(task);
            } finally {
                pollLock.unlock();
            }
        } finally {
            lock.unlock();
        }
    }

    public void scheduleNopPollTask() {
        lock.lock();
        try {
            schedulePollTask(() -> { });
        } finally {
            lock.unlock();
        }
    }

    public Set<TopicPartition> paused() {
        lock.lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(paused));
        } finally {
            lock.unlock();
        }
    }

    private void ensureNotClosed() {
        if (this.closed)
            throw new IllegalStateException("This consumer has already been closed.");
    }

    private void updateFetchPosition(TopicPartition tp) {
        if (subscriptions.isOffsetResetNeeded(tp)) {
            resetOffsetPosition(tp);
        } else if (!committed.containsKey(tp)) {
            subscriptions.requestOffsetReset(tp);
            resetOffsetPosition(tp);
        } else {
            subscriptions.seek(tp, committed.get(tp).offset());
        }
    }

    private void resetOffsetPosition(TopicPartition tp) {
        OffsetResetStrategy strategy = subscriptions.resetStrategy(tp);
        Long offset;
        if (strategy == OffsetResetStrategy.EARLIEST) {
            offset = beginningOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have beginning offset specified, but tried to seek to beginning");
        } else if (strategy == OffsetResetStrategy.LATEST) {
            offset = endOffsets.get(tp);
            if (offset == null)
                throw new IllegalStateException("MockConsumer didn't have end offset specified, but tried to seek to end");
        } else {
            throw new NoOffsetForPartitionException(tp);
        }
        seek(tp, offset);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return partitionsFor(topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return listTopics();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
            Duration timeout) {
        return offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return endOffsets(partitions);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return new ConsumerGroupMetadata("dummy.group.id", 1, "1", Optional.empty());
    }

    @Override
    public void enforceRebalance() {
        shouldRebalance = true;
    }

    public boolean shouldRebalance() {
        return shouldRebalance;
    }

    public void resetShouldRebalance() {
        shouldRebalance = false;
    }

    public Duration lastPollTimeout() {
        return lastPollTimeout;
    }

    @Override
    public void close(Duration timeout) {
        close();
    }
}
