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
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;

/**
 * @author cvarela
 * @since 0.1
 */
public class DbcopyJobBean implements Serializable {

    private static final long serialVersionUID = 3640559668991529501L;

    private static final int DEFAULT_THREADS = 1;

    @XmlAttribute(name = "id", required = true)
    private String taskId;

    @XmlAttribute(name = "threads", required = false)
    private int threads = DEFAULT_THREADS;

    @XmlElement(name = "select", type = SelectOperationBean.class, required = true)
    private SelectOperationBean selectBean;

    @XmlElement(name = "insert", type = InsertOperationBean.class, required = true)
    private InsertOperationBean insertBeanList;

    @XmlElement(name = "delete", type = DeleteOperationBean.class)
    private DeleteOperationBean deleteBean;

    @XmlElement(name = "export", type = ExportOperationBean.class)
    private ExportOperationBean exportBean;

    @XmlElements({ //
            @XmlElement(name = "integer-range", type = IntegerRangeBean.class), //
            @XmlElement(name = "literal-list", type = LiteralListBean.class) //
    })
    private List<AbstractVariableBean> variableList;

    public DeleteOperationBean getDeleteBean() {
        return deleteBean;
    }

    public ExportOperationBean getExportBean() {
        return exportBean;
    }

    public String getId() {
        return taskId;
    }

    public InsertOperationBean getInsertBean() {
        return insertBeanList;
    }

    public List<AbstractVariableBean> getVariableList() {
        return variableList;
    }

    public SelectOperationBean getSelectBean() {
        return selectBean;
    }

    public int getThreads() {
        return threads > DEFAULT_THREADS ? threads : DEFAULT_THREADS;
    }
}
