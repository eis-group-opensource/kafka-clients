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
package org.apache.kafka.common.cache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Wrapper for caches that adds simple synchronization to provide a thread-safe cache. Note that this simply adds
 * synchronization around each cache method on the underlying unsynchronized cache. It does not add any support for
 * atomically checking for existence of an entry and computing and inserting the value if it is missing.
 */
public class SynchronizedCache<K, V> implements Cache<K, V> {
    private final Cache<K, V> underlying;
    private final Lock lock = new ReentrantLock();

    public SynchronizedCache(Cache<K, V> underlying) {
        this.underlying = underlying;
    }

    @Override
    public V get(K key) {
        lock.lock();
        try {
            return underlying.get(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        lock.lock();
        try {
            underlying.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(K key) {
        lock.lock();
        try {
            return underlying.remove(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long size() {
        lock.lock();
        try {
            return underlying.size();
        } finally {
            lock.unlock();
        }
    }
}
