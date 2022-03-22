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
 * Variable representing a range of number values and allowing iteration through them.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class IntegerRangeBean extends AbstractVariableBean {

    private static final long DEFAULT_BLOCK_SIZE = 1;
    private static final long serialVersionUID = -1202210289450031802L;

    @XmlAttribute(name = "block-size")
    private long blockSize = DEFAULT_BLOCK_SIZE;

    @XmlAttribute(name = "high-value", required = true)
    private long highValue;

    @XmlAttribute(name = "low-value", required = true)
    private long lowValue;

    @Override
    public void accept(final VariableVisitor visitor) {
        visitor.visit(this);
    }

    public long getBlockSize() {
        if (blockSize < DEFAULT_BLOCK_SIZE) {
            blockSize = DEFAULT_BLOCK_SIZE;
        }
        return blockSize;
    }

    public void setBlockSize(final long blockSize) {
        this.blockSize = blockSize;
    }

    public long getHighValue() {
        return highValue;
    }

    public void setHighValue(final long highValue) {
        this.highValue = highValue;
    }

    public long getLowValue() {
        return lowValue;
    }

    public void setLowValue(final long lowValue) {
        this.lowValue = lowValue;
    }
}
