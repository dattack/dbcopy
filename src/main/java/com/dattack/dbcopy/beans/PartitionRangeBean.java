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

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * This class represents a database range-partition configuration.
 *
 * @author cvarela
 * @since 0.3
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionRangeBean {

    @XmlAttribute(name = "name", required = true)
    private String name;

    @XmlAttribute(name = "seq")
    private int sequence;

    @XmlAttribute(name = "low-value")
    private String lowValue;

    @XmlAttribute(name = "low-inclusive")
    private String lowInclusive;

    @XmlAttribute(name = "high-value")
    private String highValue;

    @XmlAttribute(name = "high-inclusive")
    private String highInclusive;

    public String getHighInclusive() {
        return highInclusive;
    }

    public void setHighInclusive(final String highInclusive) {
        this.highInclusive = highInclusive;
    }

    public Object getHighObject() {
        return getObjectValue(getHighValue());
    }

    public String getHighValue() {
        return highValue;
    }

    public void setHighValue(final String highValue) {
        this.highValue = highValue;
    }

    public String getLowInclusive() {
        return lowInclusive;
    }

    public void setLowInclusive(final String lowInclusive) {
        this.lowInclusive = lowInclusive;
    }

    public Object getLowObject() {
        return getObjectValue(getLowValue());
    }

    public String getLowValue() {
        return lowValue;
    }

    public void setLowValue(final String lowValue) {
        this.lowValue = lowValue;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    private Object getObjectValue(String text) {
        if (StringUtils.contains(text, ",")) {
            return Arrays.asList(StringUtils.split(text, ","));
        } else {
            return text;
        }
    }
}
