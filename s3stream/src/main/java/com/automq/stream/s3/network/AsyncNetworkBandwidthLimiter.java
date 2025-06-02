/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.s3.network;

import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.NetworkStats;
import com.automq.stream.utils.LogContext;
import com.automq.stream.utils.Threads;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.util.concurrent.DefaultThreadFactory;

public class AsyncNetworkBandwidthLimiter implements NetworkBandwidthLimiter {
    private static final Logger LOGGER = new LogContext().logger(AsyncNetworkBandwidthLimiter.class);
    private static final long MAX_TOKEN_PART_SIZE = 1024 * 1024;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long maxTokens;
    private final ScheduledExecutorService refillThreadPool;
    private final ExecutorService callbackThreadPool;
    private final Queue<BucketItem> queuedCallbacks;
    private final Type type;
    // 记录每次填充 token 的数量
    private final long tokenSize;
    // 记录可用的 token 数量
    private long availableTokens;

    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs) {
        this(type, tokenSize, refillIntervalMs, tokenSize);
    }

    @SuppressWarnings("this-escape")
    public AsyncNetworkBandwidthLimiter(Type type, long tokenSize, int refillIntervalMs, long maxTokens) {
        this.type = type;
        this.tokenSize = tokenSize;
        this.availableTokens = this.tokenSize;
        this.maxTokens = maxTokens;
        this.queuedCallbacks = new PriorityQueue<>();
        this.refillThreadPool =
            Threads.newSingleThreadScheduledExecutor(new DefaultThreadFactory("refill-bucket-thread"), LOGGER);
        this.callbackThreadPool = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(1, "callback-thread", true, LOGGER);
        // 启动任务
        this.callbackThreadPool.execute(this::run);
        // 启动任务，每次间隔 refillIntervalMs 填充 tokenSize 数量的 token，最大不能超过 maxTokens
        this.refillThreadPool.scheduleAtFixedRate(this::refillToken, refillIntervalMs, refillIntervalMs, TimeUnit.MILLISECONDS);
        S3StreamMetricsManager.registerNetworkLimiterQueueSizeSupplier(type, this::getQueueSize);
        LOGGER.info("AsyncNetworkBandwidthLimiter initialized, type: {}, tokenSize: {}, maxTokens: {}, refillIntervalMs: {}",
            type.getName(), tokenSize, maxTokens, refillIntervalMs);
    }

    private void run() {
        while (true) {
            lock.lock();
            try {
                // 没有异步任务，直接 await
                while (!ableToConsume()) {
                    condition.await();
                }
                // 当可用的 token 大于 0 时，开始处理
                // 因为有 refillToken 任务在跑着，所以 availableTokens 是在增加的，直到 maxTokens
                while (ableToConsume()) {
                    BucketItem head = queuedCallbacks.peek();
                    if (head == null) {
                        break;
                    }
                    // 每次最多尝试消费 1M 的数据，如果消费完了，那么将这个数据 CompletableFuture 设置为 finish，同时从队列中删除
                    long size = Math.min(head.size, MAX_TOKEN_PART_SIZE);
                    reduceToken(size);
                    if (head.complete(size)) {
                        queuedCallbacks.poll();
                    }
                }
            } catch (InterruptedException ignored) {
                break;
            } finally {
                lock.unlock();
            }
        }
    }

    private void refillToken() {
        lock.lock();
        try {
            availableTokens = Math.min(availableTokens + this.tokenSize, this.maxTokens);
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private boolean ableToConsume() {
        if (queuedCallbacks.isEmpty()) {
            return false;
        }
        return availableTokens > 0;
    }

    public void shutdown() {
        this.callbackThreadPool.shutdown();
        this.refillThreadPool.shutdown();
    }

    public long getMaxTokens() {
        return maxTokens;
    }

    public long getAvailableTokens() {
        lock.lock();
        try {
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }

    public int getQueueSize() {
        lock.lock();
        try {
            return queuedCallbacks.size();
        } finally {
            lock.unlock();
        }
    }

    private void forceConsume(long size) {
        lock.lock();
        try {
            reduceToken(size);
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> consume(ThrottleStrategy throttleStrategy, long size) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        // 完成后记录 metrics
        cf.whenComplete((v, e) -> NetworkStats.getInstance().networkUsageTotalStats(type, throttleStrategy).add(MetricsLevel.INFO, size));
        if (Objects.requireNonNull(throttleStrategy) == ThrottleStrategy.BYPASS) {
            // 强制消费
            forceConsume(size);
            cf.complete(null);
        } else {
            lock.lock();
            try {
                // 没有可用的 token 或者已经有排队的请求了，添加到异步队列中
                if (availableTokens <= 0 || !queuedCallbacks.isEmpty()) {
                    queuedCallbacks.offer(new BucketItem(throttleStrategy, size, cf));
                    // 唤醒异步消费任务
                    condition.signalAll();
                } else {
                    reduceToken(size);
                    cf.complete(null);
                }
            } finally {
                lock.unlock();
            }
        }
        return cf;
    }

    private void reduceToken(long size) {
        this.availableTokens = Math.max(-maxTokens, availableTokens - size);
    }

    public enum Type {
        INBOUND("Inbound"),
        OUTBOUND("Outbound");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static class BucketItem implements Comparable<BucketItem> {
        // 策略，数字越小优先级越高
        private final ThrottleStrategy strategy;
        private final CompletableFuture<Void> cf;
        private final long timestamp;
        private long size;

        BucketItem(ThrottleStrategy strategy, long size, CompletableFuture<Void> cf) {
            this.strategy = strategy;
            this.size = size;
            this.cf = cf;
            this.timestamp = System.nanoTime();
        }

        @Override
        public int compareTo(BucketItem o) {
            if (strategy.priority() == o.strategy.priority()) {
                return Long.compare(timestamp, o.timestamp);
            }
            return Long.compare(strategy.priority(), o.strategy.priority());
        }

        public boolean complete(long completeSize) {
            size -= completeSize;
            if (size <= 0) {
                cf.complete(null);
                return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (BucketItem) obj;
            return this.strategy == that.strategy &&
                this.size == that.size &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(strategy, size, cf);
        }

        @Override
        public String toString() {
            return "BucketItem[" +
                "throttleStrategy=" + strategy + ", " +
                "size=" + size + ", " +
                "cf=" + cf + ']';
        }

    }
}
