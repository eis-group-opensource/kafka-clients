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
package org.apache.kafka.common.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 */
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    private volatile Map<K, V> map;
    private final Lock lock = new ReentrantLock();

    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            this.map = Collections.emptyMap();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V put(K k, V v) {
        lock.lock();
        try {
            Map<K, V> copy = new HashMap<K, V>(this.map);
            V prev = copy.put(k, v);
            this.map = Collections.unmodifiableMap(copy);
            return prev;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        lock.lock();
        try {
            Map<K, V> copy = new HashMap<K, V>(this.map);
            copy.putAll(entries);
            this.map = Collections.unmodifiableMap(copy);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V remove(Object key) {
        lock.lock();
        try {
            Map<K, V> copy = new HashMap<K, V>(this.map);
            V prev = copy.remove(key);
            this.map = Collections.unmodifiableMap(copy);
            return prev;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V putIfAbsent(K k, V v) {
        lock.lock();
        try {
            if (!containsKey(k))
                return put(k, v);
            else
                return get(k);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object k, Object v) {
        lock.lock();
        try {
            if (containsKey(k) && get(k).equals(v)) {
                remove(k);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean replace(K k, V original, V replacement) {
        lock.lock();
        try {
            if (containsKey(k) && get(k).equals(original)) {
                put(k, replacement);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public V replace(K k, V v) {
        lock.lock();
        try {
            if (containsKey(k)) {
                return put(k, v);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

}
