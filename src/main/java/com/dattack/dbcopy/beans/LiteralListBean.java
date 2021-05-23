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

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Variable representing a set of literal values and allowing iteration through them.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class LiteralListBean extends AbstractVariableBean {

    private static final int DEFAULT_BLOCK_SIZE = 1;
    private static final long serialVersionUID = 5767020179827950329L;

    @XmlAttribute(name = "block-size")
    private int blockSize;

    @XmlAttribute(name = "values", required = true)
    private String values;

    public LiteralListBean() {
        super();
        this.blockSize = DEFAULT_BLOCK_SIZE;
    }

    @Override
    public void accept(final VariableVisitor visitor) {
        visitor.visit(this);
    }

    public int getBlockSize() {
        if (blockSize < DEFAULT_BLOCK_SIZE) {
            blockSize = DEFAULT_BLOCK_SIZE;
        }
        return blockSize;
    }

    public void setBlockSize(final int blockSize) {
        this.blockSize = blockSize;
    }

    public List<String> getValues() {

        final List<String> list = new ArrayList<>();
        final String[] items = values.split(",");
        for (final String item : items) {
            list.add(item.trim());
        }
        return list;
    }

    public void setValues(final String values) {
        this.values = values;
    }
}
