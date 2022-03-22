/*
 * Copyright (c) 2022, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.automator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents a partition by-range of a table.
 *
 * @author cvarela
 * @since 0.3
 */
public class RangePartition {

    private final String partitionName;
    private final String lowValue;
    private final InclusiveMode lowInclusiveMode;
    private final String highValue;
    private final InclusiveMode highInclusiveMode;
    private final int position;
    private final int numRows;

    public RangePartition(String partitionName, String lowValue, Boolean lowInclusiveMode, String highValue,
        Boolean highInclusiveMode, int position, int numRows)
    {
        this.partitionName = partitionName;
        this.lowValue = lowValue;
        this.lowInclusiveMode = createInclusiveMode(lowInclusiveMode);
        this.highValue = highValue;
        this.highInclusiveMode = createInclusiveMode(highInclusiveMode);
        this.position = position;
        this.numRows = numRows;
    }

    public RangePartition(String partitionName, String highValue, int position, int numRows) {
        this(partitionName, null, null, highValue, null, position, numRows);
    }

    private static InclusiveMode createInclusiveMode(Boolean mode) {
        InclusiveMode result = InclusiveMode.UNSET;
        if (mode != null) {
            result = mode ? InclusiveMode.INCLUSIVE : InclusiveMode.EXCLUSIVE;
        }
        return result;
    }

    public InclusiveMode getHighInclusiveMode() {
        return highInclusiveMode;
    }

    public String getHighValue() {
        return highValue;
    }

    public InclusiveMode getLowInclusiveMode() {
        return lowInclusiveMode;
    }

    public String getLowValue() {
        return lowValue;
    }

    public int getNumRows() {
        return numRows;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getPosition() {
        return position;
    }

    public boolean isSize1() {
        return StringUtils.equalsIgnoreCase(getLowValue(), getHighValue());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this) //
            .append("partitionName", partitionName) //
            .append("lowValue", lowValue) //
            .append("lowInclusive", lowInclusiveMode) //
            .append("highValue", highValue) //
            .append("highInclusive", highInclusiveMode) //
            .append("position", position) //
            .append("numRows", numRows) //
            .toString();
    }

    public enum InclusiveMode {
        INCLUSIVE, EXCLUSIVE, MIXED, UNSET
    }
}
