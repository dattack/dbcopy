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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;

/**
 * Bean representing a job configuration.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DbcopyJobBean implements Serializable {

    private static final int DEFAULT_THREADS = 1;
    private static final long serialVersionUID = 3640559668991529501L;

    @XmlElement(name = "delete", type = DeleteOperationBean.class)
    private DeleteOperationBean deleteBean;

    @XmlElement(name = "export", type = ExportOperationBean.class)
    private ExportOperationBean exportBean;

    @XmlAttribute(name = "id", required = true)
    private String id;

    @XmlElement(name = "insert", type = InsertOperationBean.class, required = true)
    private InsertOperationBean insertBean;

    @XmlElement(name = "select", type = SelectOperationBean.class, required = true)
    private SelectOperationBean selectBean;

    @XmlAttribute(name = "threads")
    private int threads = DEFAULT_THREADS;

    @XmlElements({ //
            @XmlElement(name = "integer-range", type = IntegerRangeBean.class), //
            @XmlElement(name = "literal-list", type = LiteralListBean.class) //
    })
    private List<AbstractVariableBean> variableList;

    public DeleteOperationBean getDeleteBean() {
        return deleteBean;
    }

    public void setDeleteBean(final DeleteOperationBean deleteBean) {
        this.deleteBean = deleteBean;
    }

    public ExportOperationBean getExportBean() {
        return exportBean;
    }

    public void setExportBean(final ExportOperationBean exportBean) {
        this.exportBean = exportBean;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public InsertOperationBean getInsertBean() {
        return insertBean;
    }

    public void setInsertBean(final InsertOperationBean insertBean) {
        this.insertBean = insertBean;
    }

    public SelectOperationBean getSelectBean() {
        return selectBean;
    }

    public void setSelectBean(final SelectOperationBean selectBean) {
        this.selectBean = selectBean;
    }

    public int getThreads() {
        return threads > DEFAULT_THREADS ? threads : DEFAULT_THREADS;
    }

    public void setThreads(final int threads) {
        this.threads = threads;
    }

    public List<AbstractVariableBean> getVariableList() {
        return variableList;
    }

    public void setVariableList(final List<AbstractVariableBean> variableList) {
        this.variableList = variableList;
    }
}
