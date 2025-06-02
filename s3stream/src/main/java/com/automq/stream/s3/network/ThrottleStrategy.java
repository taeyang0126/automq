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

public enum ThrottleStrategy {
    /**
     * BYPASS (优先级 0，最高)
     * 直接通过，最高优先级
     * 用于正常的、实时的消息写入
     * 不希望被延迟的核心业务流程
     * 例如：生产者实时写入消息
     */
    BYPASS(0, "bypass"),
    /**
     * COMPACTION (优先级 1)
     * 用于日志压缩、清理等维护操作
     * 重要但不是最紧急的任务
     * 例如：合并小文件、删除过期数据等维护操作
     */
    COMPACTION(1, "compaction"),
    /**
     * TAIL (优先级 2)
     * 用于尾部追加操作
     * 通常是实时但可以稍微延迟的操作
     * 例如：实时但非核心的数据写入
     */
    TAIL(2, "tail"),
    /**
     * CATCH_UP (优先级 3，最低)
     * 用于追赶、同步历史数据
     * 可以被延迟的后台操作
     * 例如：消费者落后太多时的追赶操作、副本同步等
     */
    CATCH_UP(3, "catchup");

    private final int priority;
    private final String name;

    ThrottleStrategy(int priority, String name) {
        this.priority = priority;
        this.name = name;
    }

    public int priority() {
        return priority;
    }

    public String getName() {
        return name;
    }
}
