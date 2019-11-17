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
import java.io.Serializable;

/**
 * @author cvarela
 * @since 0.1
 */
public class ExportOperationBean implements Serializable {

    private static final long serialVersionUID = 75388957947238367L;

    @XmlAttribute(name = "path", required = true)
    private String path;

    @XmlAttribute(name = "type", required = false)
    private String type = "csv";

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }
}
