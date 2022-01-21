/*
 * Copyright (c) 2022, The Dattack team (http://www.dattack.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dattack.dbcopy.engine;

/**
 * MBean to access the internal queue used to transfer data between producers and consumers.
 *
 * @author cvarela
 * @since 0.1
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface TransferQueueWrapperMBean {

    /**
     * Returns the number of elements in the internal TransferQueue.
     *
     * @see java.util.concurrent.TransferQueue#size()
     * @return the number of elements in the internal TransferQueue.
     */
    int getSize();

    /**
     * Returns an estimate of the number of consumers waiting to receive elements from an internal TransferQueue.
     *
     * @see java.util.concurrent.TransferQueue#getWaitingConsumerCount()
     * @return the number of consumers waiting to receive elements
     */
    int getWaitingConsumerCount();

    /**
     * Returns {@code true} if there is at least one waiting consumer.
     *
     * @see java.util.concurrent.TransferQueue#hasWaitingConsumer()
     * @return {@code true} if there is at least one waiting consumer
     */
    boolean hasWaitingConsumer();
}
