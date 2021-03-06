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

/**
 * @author cvarela
 * @since 0.1
 */
public class ExecutionController implements ExecutionControllerMBean {

    private final AtomicInteger counter = new AtomicInteger();
    private final String name;
    private final ThreadPoolExecutor threadPoolExecutor;

    public ExecutionController(final String name, final int corePoolSize, final int maximumPoolSize) {
        this.name = name;
        this.threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(), getThreadFactory());
    }

    @Override
    public int getCorePoolSize() {
        return threadPoolExecutor.getCorePoolSize();
    }

    @Override
    public int getMaximumPoolSize() {
        return threadPoolExecutor.getMaximumPoolSize();
    }

    private ThreadFactory getThreadFactory() {

        return new ThreadFactory() {

            @Override
            public Thread newThread(final Runnable target) {
                return new Thread(target, String.format("%s-%d", name, counter.getAndIncrement()));
            }
        };
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
