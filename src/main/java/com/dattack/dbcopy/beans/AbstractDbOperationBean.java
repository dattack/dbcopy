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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;
import java.io.Serializable;

/**
 * Abstract class containing the common properties of all the operations executed by DBCopy.
 *
 * @author cvarela
 * @since 0.1
 */
public abstract class AbstractDbOperationBean implements Serializable {

    private static final long serialVersionUID = -3160515038190663322L;

    @XmlAttribute(name = "datasource", required = true)
    private String datasource;

    @XmlValue
    private String sql;

    public String getDatasource() {
        return datasource;
    }

    public String getSql() {
        return sql;
    }
}
