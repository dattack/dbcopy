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
package com.dattack.dbcopy.engine;

import java.sql.SQLException;

/**
 * @author cvarela
 * @since 0.1
 */
final class DbCopyTaskResult {

    private int retrievedRows;
    private int insertedRows;
    private final RangeValue rangeValue;
    private SQLException exception;

    public DbCopyTaskResult(final RangeValue rangeValue) {
        this.rangeValue = rangeValue;
        this.retrievedRows = 0;
        this.insertedRows = 0;
        this.exception = null;
    }

    public void addInsertedRows(final int value) {
        this.insertedRows += value;
    }

    public void incrementRetrievedRows() {
        this.retrievedRows += 1;
    }

    public SQLException getException() {
        return exception;
    }

    public int getInsertedRows() {
        return insertedRows;
    }

    public RangeValue getRangeValue() {
        return rangeValue;
    }

    public int getRetrievedRows() {
        return retrievedRows;
    }

    public void setException(final SQLException exception) {
        this.exception = exception;
    }

    @Override
    public String toString() {

        final StringBuilder str = new StringBuilder().append("DbCopyTaskResult [rangeValue=").append(rangeValue)
                .append(", retrievedRows=").append(retrievedRows) //
                .append(", insertedRows=").append(insertedRows);

        if (exception != null) {
            str.append(", exception=").append(exception);
        }
        str.append(']');
        return str.toString();
    }

}
