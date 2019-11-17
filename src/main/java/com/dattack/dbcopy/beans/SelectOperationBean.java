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

/**
 * @author cvarela
 * @since 0.1
 */
public class SelectOperationBean extends AbstractDbOperationBean {

    private static final long serialVersionUID = -8426358006541063367L;

    private static final int DEFAULT_FETCH_SIZE = 0;

    @XmlAttribute(name = "fetch-size", required = false)
    private int fetchSize = DEFAULT_FETCH_SIZE;

    public int getFetchSize() {
        return fetchSize > DEFAULT_FETCH_SIZE ? fetchSize : DEFAULT_FETCH_SIZE;
    }
}
