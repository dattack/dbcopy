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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Bean representing a insert operation.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class InsertOperationBean extends AbstractDbOperationBean {

    private static final int DEFAULT_BATCH_SIZE = 0;
    private static final int DEFAULT_PARALLEL = 1;
    private static final long serialVersionUID = -1303451998596082687L;

    @XmlAttribute(name = "batch-size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @XmlAttribute(name = "parallel")
    private int parallel = DEFAULT_PARALLEL;

    @XmlAttribute(name = "table")
    private String table;

    public int getBatchSize() {
        return batchSize > DEFAULT_BATCH_SIZE ? batchSize : DEFAULT_BATCH_SIZE;
    }

    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    public int getParallel() {
        return parallel > DEFAULT_PARALLEL ? parallel : DEFAULT_PARALLEL;
    }

    public void setParallel(final int parallel) {
        this.parallel = parallel;
    }

    public String getTable() {
        return table;
    }

    public void setTable(final String table) {
        this.table = table;
    }
}
