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

import javax.xml.bind.annotation.XmlAttribute;

/**
 * @author cvarela
 * @since 0.1
 */
public class LiteralListBean extends AbstractVariableBean {

    private static final int DEFAULT_BLOCK_SIZE = 1;

    @XmlAttribute(name = "values", required = true)
    private String values;

    @XmlAttribute(name = "block-size", required = false)
    private int blockSize;

    public LiteralListBean() {
        this.blockSize = DEFAULT_BLOCK_SIZE;
    }

    public List<Integer> getValues() {

        List<Integer> list = new ArrayList<>();
        String[] items = values.split(",");
        for (String item : items) {
            list.add(Integer.valueOf(item.trim()));
        }
        return list;
    }

    @Override
    public void accept(final VariableVisitor visitor) {
        visitor.visite(this);
    }

    public int getBlockSize() {
        if (blockSize < DEFAULT_BLOCK_SIZE) {
            return DEFAULT_BLOCK_SIZE;
        }
        return blockSize;
    }
}
