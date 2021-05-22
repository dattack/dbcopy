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

/**
 * MBean to manage a thread pool.
 *
 * @author cvarela
 * @since 0.1
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface ExecutionControllerMBean {

    public int getCorePoolSize();

    public int getMaximumPoolSize();

    public void setCorePoolSize(final int size);

    public void setMaximumPoolSize(final int size);

    public void shutdown();

    public <T> Future<T> submit(final Callable<T> task);
}
