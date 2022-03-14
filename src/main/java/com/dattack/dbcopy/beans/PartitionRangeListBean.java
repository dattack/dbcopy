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

import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * Variable representing a list of database range-partitions and allowing iteration through them.
 *
 * @author cvarela
 * @since 0.3
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class PartitionRangeListBean extends AbstractVariableBean {

    private static final long serialVersionUID = 5767020179827950329L;

    @XmlElement(name = "partition", required = true)
    private List<PartitionRangeBean> partitionRangeBeanList;

    public PartitionRangeListBean() {
        super();
    }

    @Override
    public void accept(final VariableVisitor visitor) {
        visitor.visit(this);
    }

    public List<PartitionRangeBean> getPartitionBeanList() {
        return partitionRangeBeanList;
    }

    public void setPartitionBeanList(final List<PartitionRangeBean> partitionRangeBeanList) {
        this.partitionRangeBeanList = partitionRangeBeanList;
    }
}
