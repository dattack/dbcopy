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
public class IntegerRangeBean extends AbstractVariableBean {

    private static final long DEFAULT_BLOCK_SIZE = 1;

    @XmlAttribute(name = "low-value", required = true)
    private long lowValue;

    @XmlAttribute(name = "high-value", required = true)
    private long highValue;

    @XmlAttribute(name = "block-size")
    private long blockSize = DEFAULT_BLOCK_SIZE;

    @Override
    public void accept(final VariableVisitor visitor) {
        visitor.visite(this);
    }

    public long getBlockSize() {
        if (blockSize < DEFAULT_BLOCK_SIZE) {
            blockSize = DEFAULT_BLOCK_SIZE;
        }
        return blockSize;
    }

    public long getHighValue() {
        return highValue;
    }

    public long getLowValue() {
        return lowValue;
    }
}
