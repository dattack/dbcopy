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

/**
 * @author cvarela
 * @since 0.1
 */
class IntegerRangeValue implements RangeValue {

    private static final long serialVersionUID = -2375366347089643655L;

    private final int lowValue;
    private final int highValue;

    public IntegerRangeValue(final int lowValue, final int highValue) {
        this.lowValue = lowValue;
        this.highValue = highValue;
    }

    public int getHighValue() {
        return highValue;
    }

    public int getLowValue() {
        return lowValue;
    }

    @Override
    public String toString() {
        return "IntegerRangeValue [lowValue=" + lowValue + ", highValue=" + highValue + "]";
    }
}
