/*
 * Copyright (c) 2019, The Dattack team (http://www.dattack.com)
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
 * Set of metadata corresponding to a column returned by {@link DataTransfer#transfer()}.
 *
 * @author cvarela
 * @since 0.1
 */
public class ColumnMetadata {

    private String name;
    private int index;
    private int type;
    private int scale;

    public ColumnMetadata(String name, int index, int type, int scale) {
        this.name = name;
        this.index = index;
        this.type = type;
        this.scale = scale;
    }

    public int getIndex() {
        return index;
    }

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public String toString() {
        return new StringBuilder("ColumnMetadata{") //
                .append("name='").append(getName()).append('\'') //
                .append(", index=").append(getIndex()) //
                .append(", type=").append(getType()) //
                .append(", scale=").append(getScale()) //
                .append('}')
                .toString();
    }
}
