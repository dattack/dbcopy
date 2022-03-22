/*
 * Copyright (c) 2017, The Dattack team (http://www.dattack.com)
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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectInstance;

/**
 * MBean implementation to manage a thread pool.
 *
 * @author cvarela
 * @since 0.1
 */
public class ExecutionController implements ExecutionControllerMBean, AutoCloseable {

    private static final String THREAD_POOL_TYPE = "ThreadPool";

    private final transient AtomicInteger threadCounter = new AtomicInteger();
    private final transient ThreadPoolExecutor threadPoolExecutor;
    private final transient ObjectInstance objectInstance;

    public ExecutionController(final String name, final int poolSize) {
        this(name, poolSize, poolSize);
    }

    public ExecutionController(final String name, final int corePoolSize, final int maximumPoolSize) {
        this.threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(), getThreadFactory(name));
        objectInstance = MBeanHelper.registerMBean(THREAD_POOL_TYPE, name, this);
    }

    @Override
    public void close() {
        if (objectInstance != null) {
            MBeanHelper.unregisterMBean(objectInstance.getObjectName());
        }
    }

    private ThreadFactory getThreadFactory(String name) {
        return target -> new Thread(target, String.format("%s-%d", name, threadCounter.getAndIncrement()));
    }

    @Override
    public int getCorePoolSize() {
        return threadPoolExecutor.getCorePoolSize();
    }

    @Override
    public int getMaximumPoolSize() {
        return threadPoolExecutor.getMaximumPoolSize();
    }

    @Override
    public void setCorePoolSize(final int size) {
        if (size > 0) {
            threadPoolExecutor.setCorePoolSize(size);
        }
    }

    @Override
    public void setMaximumPoolSize(final int size) {
        if (size >= threadPoolExecutor.getCorePoolSize()) {
            threadPoolExecutor.setMaximumPoolSize(size);
        }
    }

    @Override
    public void shutdown() {
        this.threadPoolExecutor.shutdown();
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
        return this.threadPoolExecutor.submit(task);
    }
}
