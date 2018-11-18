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
package com.dattack.dbcopy.beans;

import javax.xml.bind.annotation.XmlAttribute;

/**
 * @author cvarela
 * @since 0.1
 */
public class InsertOperationBean extends AbstractOperationBean {

    private static final long serialVersionUID = -1303451998596082687L;

    private static final int DEFAULT_BATCH_SIZE = 0;
    private static final int DEFAULT_PARALLEL = 1;

    @XmlAttribute(name = "batch-size", required = false)
    private int batchSize = DEFAULT_BATCH_SIZE;

    @XmlAttribute(name = "parallel", required = false)
    private int parallel = DEFAULT_PARALLEL;

    @Override
    public void accept(final OperationBeanVisitor visitor) {
        visitor.visite(this);
    }

    public int getBatchSize() {
        return batchSize > DEFAULT_BATCH_SIZE ? batchSize : DEFAULT_BATCH_SIZE;
    }

    public int getParallel() {
        return parallel > DEFAULT_PARALLEL ? parallel : DEFAULT_PARALLEL;
    }
}
