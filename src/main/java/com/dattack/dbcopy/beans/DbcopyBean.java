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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Bean representing a dbcopy configuration file.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlRootElement(name = "dbcopy")
@XmlAccessorType(XmlAccessType.FIELD)
public class DbcopyBean implements Serializable {

    private static final long serialVersionUID = 8398044544048577943L;

    @XmlElement(name = "job", required = true, type = DbcopyJobBean.class)
    private final List<DbcopyJobBean> jobList;

    @XmlAttribute(name = "parallel")
    private int parallel = 1;

    public DbcopyBean() {
        this.jobList = new ArrayList<>();
    }

    public List<DbcopyJobBean> getJobList() {
        return jobList;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(final int parallel) {
        this.parallel = parallel;
    }
}
