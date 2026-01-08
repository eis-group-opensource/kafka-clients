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
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
 * A thread-safe helper class to hold batches that haven't been acknowledged yet (including those
 * which have and have not been sent).
 */
class IncompleteBatches {
    private final Set<ProducerBatch> incomplete;
    private final Lock lock = new ReentrantLock();
    
    public IncompleteBatches() {
        this.incomplete = new HashSet<>();
    }

    public void add(ProducerBatch batch) {
        lock.lock();
        try {
            this.incomplete.add(batch);
        } finally {
            lock.unlock();
        }
    }

    public void remove(ProducerBatch batch) {
        lock.lock();
        try {
            boolean removed = this.incomplete.remove(batch);
            if (!removed)
                throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
        } finally {
            lock.unlock();
        }
    }

    public Iterable<ProducerBatch> copyAll() {
        lock.lock();
        try {
            return new ArrayList<>(this.incomplete);
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return incomplete.isEmpty();
        } finally {
            lock.unlock();
        }
    }
}
