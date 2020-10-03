/*
 * Copyright (c) 2020, The Dattack team (http://www.dattack.com)
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
package com.dattack.dbcopy.engine.export.csv;

import com.dattack.dbcopy.beans.ExportOperationBean;
import com.dattack.jtoolbox.commons.configuration.ConfigurationUtil;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * @author cvarela
 * @since 0.2
 */
class CsvExportWriteWrapper implements Closeable {

    private final static Logger LOGGER = LoggerFactory.getLogger(CsvExportWriteWrapper.class);

    private ExportOperationBean bean;
    private AbstractConfiguration configuration;
    private volatile Path path;
    private volatile Writer writer;
    private String header;
    private volatile AtomicInteger fileNumber;
    private final Object lock;
    private volatile CountingOutputStream cos;

    public CsvExportWriteWrapper(ExportOperationBean bean, AbstractConfiguration configuration) {
        this.bean = bean;
        this.configuration = configuration;
        this.fileNumber = bean.getRotateSize() > 0 ? new AtomicInteger() : null;
        this.lock = new Object();
    }

    private void init() throws IOException {

        String filename = ConfigurationUtil.interpolate(bean.getPath(), configuration);
        if (fileNumber != null) {
            filename = createPartFilename(filename, fileNumber.get());
            fileNumber.incrementAndGet();
        }

        Path path = Paths.get(filename);
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        OutputStream outputStream =  Files.newOutputStream(path,
                StandardOpenOption.CREATE, //
                StandardOpenOption.WRITE, //
                StandardOpenOption.TRUNCATE_EXISTING);

        cos = new CountingOutputStream(outputStream);
        outputStream = cos;
        if (bean.isGzip()) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        this.path = path;
        if (bean.getBufferSize() > 0) {
            this.writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8),
                    bean.getBufferSize());
        } else {
            this.writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        }
    }

    private static String createPartFilename(String filename, int fileNumber) {
        String baseName = FilenameUtils.getBaseName(filename);
        String extension = FilenameUtils.getExtension(filename);

        String partFilename;

        int dotIndex = baseName.indexOf(".");
        if (dotIndex > 0) {
            partFilename = FilenameUtils.getFullPath(filename) + baseName.substring(0, dotIndex) + "_" + fileNumber //
                    + baseName.substring(dotIndex) + "." + extension;
        } else {
            partFilename = filename + "_" + fileNumber;
        }

        return partFilename;
    }

    private Writer getWriter() throws IOException {
        if (writer == null) {
            synchronized (lock) {
                if (writer == null) {
                    init();
                    // write header
                    if (StringUtils.isNotBlank(header)) {
                        writer.write(header);
                    }
                }
            }
        }
        return writer;
    }

    void setHeader(String header) {
        this.header = header;
    }

    void write(String text) throws IOException {
        synchronized (lock) {
            getWriter().write(text);
        }
        if (fileNumber != null) {
            synchronized (lock) {
                if (cos != null && cos.getByteCount() > bean.getRotateSize()) {
                    LOGGER.debug("Rotate file: {}, bytes: {}, limit: {}", path,  cos.getByteCount(),
                            bean.getRotateSize());
                    close();
                }
            }
        }
    }

    public void close() throws IOException {
        if (writer != null) {
            synchronized (lock) {
                if (writer != null) {
                    writer.close();
                    writer = null;

                    // move part file to other directory
                    if (StringUtils.isNotBlank(bean.getMove2path())) {
                        Path newDir = Paths.get(bean.getMove2path());
                        Path target = newDir.resolve(this.path.getFileName());
                        LOGGER.info("Moving file {} to {}", this.path, target);
                        Files.move(this.path, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ExportWriteWrapper{path=" + path + '}';
    }
}
