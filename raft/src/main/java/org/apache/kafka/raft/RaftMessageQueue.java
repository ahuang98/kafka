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
package org.apache.kafka.raft;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * This class is used to serialize inbound requests or responses to outbound requests.
 * It basically just allows us to wrap a blocking queue so that we can have a mocked
 * implementation which does not depend on system time.
 *
 * See {@link org.apache.kafka.raft.internals.BlockingMessageQueue}.
 */
public interface RaftMessageQueue {

    /**
     * Block for the arrival of a new message.
     *
     * @param timeoutMs timeout in milliseconds to wait for a new event
     * @return the event or {@code Optional.empty()} if either the timeout was reached or there was
     *     a call to {@link #wakeup()} before any events became available
     */
    Optional<MessageEntry> poll(long timeoutMs);

    /**
     * Add a new message to the queue.
     *
     * @param message the message to deliver
     * @return a completion stage that will be completed when the message is processed
     * @throws IllegalStateException if the queue cannot accept the message
     */
    CompletionStage<RaftMessage> add(RaftMessage message);

    /**
     * Check whether there are pending messages awaiting delivery.
     *
     * @return if there are no pending messages to deliver
     */
    boolean isEmpty();

    /**
     * Wakeup the thread blocking in {@link #poll(long)}. This will cause
     * {@link #poll(long)} to return null if no messages are available.
     */
    void wakeup();

    /**
     * An element stored in the queue, a real message or a wakeup signal.
     */
    sealed interface QueueEntry permits MessageEntry, WakeupEntry { }

    /**
     * A queue entry that carries a message and the future to complete when it is processed.
     */
    final class MessageEntry implements QueueEntry {
        private final CompletableFuture<RaftMessage> future = new CompletableFuture<>();
        private final RaftMessage message;

        public MessageEntry(RaftMessage message) {
            this.message = message;
        }

        public RaftMessage message() {
            return message;
        }

        public CompletableFuture<RaftMessage> future() {
            return future;
        }

        @Override
        public String toString() {
            return String.format(
                "MessageEntry(message=%s, future.isDone=%s)",
                message,
                future.isDone()
            );
        }
    }

    /**
     * A signal used to unblock {@link #poll(long)}. This is a single shared instance
     * used for every wakeup that is drained by (and not returned by) {@link #poll(long)}.
     */
    enum WakeupEntry implements QueueEntry {
        INSTANCE
    }
}
