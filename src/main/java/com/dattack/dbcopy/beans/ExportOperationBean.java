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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.Locale;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * bean representing an export operation.
 *
 * @author cvarela
 * @since 0.1
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ExportOperationBean implements Serializable {

    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_PAGE_SIZE = 1_048_576; // in bytes (default: 1048576 = 1024 * 1024)
    private static final int DEFAULT_PARALLEL = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportOperationBean.class);
    private static final long serialVersionUID = 75388957947238367L;

    @XmlAttribute(name = "batch-size")
    private int batchSize = DEFAULT_BATCH_SIZE;

    @XmlAttribute(name = "buffer-size")
    private int bufferSize = -1;

    @XmlAttribute(name = "compression")
    @XmlJavaTypeAdapter(CompressionAdapter.class)
    private Compression compression = Compression.UNCOMPRESSED;

    @XmlAttribute(name = "format-file")
    private String formatFile;

    @Deprecated
    @XmlAttribute(name = "gzip")
    private Boolean gzip = Boolean.FALSE;

    @XmlAttribute(name = "move-to")
    private String move2path;

    @XmlAttribute(name = "page-size")
    private int pageSize = DEFAULT_PAGE_SIZE;

    @XmlAttribute(name = "parallel")
    private int parallel = DEFAULT_PARALLEL;

    @XmlAttribute(name = "path", required = true)
    private String path;

    @XmlAttribute(name = "rotate-size")
    private long rotateSize = -1;

    @XmlAttribute(name = "type")
    @XmlJavaTypeAdapter(TypeAdapter.class)
    private Type type = Type.CSV;

    public int getBatchSize() {
        return batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
    }

    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(final int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(final Compression compression) {
        this.compression = compression;
    }

    public String getFormatFile() {
        return formatFile;
    }

    public void setFormatFile(final String formatFile) {
        this.formatFile = formatFile;
    }

    public String getMove2path() {
        return move2path;
    }

    public void setMove2path(final String move2path) {
        this.move2path = move2path;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(final int pageSize) {
        this.pageSize = pageSize;
    }

    public int getParallel() {
        return parallel > DEFAULT_PARALLEL ? parallel : DEFAULT_PARALLEL;
    }

    public void setParallel(final int parallel) {
        this.parallel = parallel;
    }

    public String getPath() {
        return path;
    }

    public void setPath(final String path) {
        this.path = path;
    }

    public long getRotateSize() {
        return rotateSize;
    }

    public void setRotateSize(final long rotateSize) {
        this.rotateSize = rotateSize;
    }

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public boolean isGzip() {
        return gzip;
    }

    @XmlAttribute(name = "gzip")
    public void setGzip(final Boolean value) {
        if (value != null) {
            LOGGER.warn("Check your configuration: the 'gzip' attribute is deprecated, use 'compression' instead.");
            this.compression = value ? Compression.GZIP : Compression.UNCOMPRESSED;
            this.gzip = value;
        }
    }

    public enum Type {
        CSV, PARQUET
    }

    public enum Compression {
        UNCOMPRESSED, SNAPPY, GZIP, LZO
    }

    /**
     * Adapts a {@link Type} for custom marshaling.
     */
    public static class TypeAdapter extends XmlAdapter<String, Type> {

        @Override
        public Type unmarshal(final String type) {

            Type result = Type.CSV;
            if (StringUtils.isNotBlank(type)) {
                result = Type.valueOf(type.toUpperCase(Locale.getDefault()));
            }
            return result;
        }

        @Override
        public String marshal(final Type status) {

            return status.name();
        }
    }

    /**
     * Adapts a {@link Compression} for custom marshaling.
     */
    public static class CompressionAdapter extends XmlAdapter<String, Compression> {

        @Override
        public Compression unmarshal(final String type) {

            Compression result = Compression.UNCOMPRESSED;
            if (StringUtils.isNotBlank(type)) {
                result = Compression.valueOf(type.toUpperCase(Locale.getDefault()));
            }
            return result;
        }

        @Override
        public String marshal(final Compression compression) {
            return compression.name();
        }
    }
}
