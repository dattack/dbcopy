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

    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_PARALLEL = 1;

    @XmlAttribute(name = "path", required = true)
    private String path;

    @XmlAttribute(name = "type", required = false)
    private String type = "csv";

    @XmlAttribute(name = "gzip", required = false)
    private Boolean gzip = Boolean.FALSE;

    @XmlAttribute(name = "parallel", required = false)
    private int parallel = DEFAULT_PARALLEL;

    @XmlAttribute(name = "batch-size", required = false)
    private int batchSize = DEFAULT_BATCH_SIZE;

    @XmlAttribute(name = "format-file", required = false)
    private String formatFile;

    @XmlAttribute(name = "rotate-size", required = false)
    private long rotateSize = -1;

    @XmlAttribute(name = "buffer-size", required = false)
    private int bufferSize = -1;

    @XmlAttribute(name = "move-to", required = false)
    private String move2path;

    public String getPath() {
        return path;
    }

    public String getType() {
        return type;
    }

    public boolean isGzip() {
        return gzip;
    }

    public int getBatchSize() {
        return batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
    }

    public int getParallel() {
        return parallel > DEFAULT_PARALLEL ? parallel : DEFAULT_PARALLEL;
    }

    public String getFormatFile() {
        return formatFile;
    }

    public long getRotateSize() {
        return rotateSize;
    }

    public String getMove2path() {
        return move2path;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
