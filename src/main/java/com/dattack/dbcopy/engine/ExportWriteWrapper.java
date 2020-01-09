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
package com.dattack.dbcopy.engine;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author cvarela
 * @since 0.2
 */
public class ExportWriteWrapper implements Closeable {

    private volatile AtomicBoolean header;
    private Path path;
    private Writer writer;

    public ExportWriteWrapper(Path path, Writer writer) {
        this.path = path;
        this.writer = writer;
        this.header = new AtomicBoolean(false);
    }

    public Path getPath() {
        return path;
    }

    public void open(String headerText) throws IOException {
        if (header.compareAndSet(false, true)) {
            writer.write(headerText);
        }
    }

    public void write(String text) throws IOException {
        writer.write(text);
    }

    public void close() throws IOException {
        writer.close();
    }

    @Override
    public String toString() {
        return "ExportWriteWrapper{" +
                "header=" + header +
                ", path=" + path +
                '}';
    }
}
