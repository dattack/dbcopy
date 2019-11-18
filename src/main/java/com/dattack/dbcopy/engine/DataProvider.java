package com.dattack.dbcopy.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.dattack.formats.csv.CSVStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;

public class DataProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);

    private ResultSet resultSet;
    private DbCopyTaskResult taskResult;
    private volatile ArrayList<Integer> columnIndexes;
    private ResultSetMetaData metadataCache;

    DataProvider(ResultSet resultSet, DbCopyTaskResult taskResult) throws SQLException {
        this.resultSet = resultSet;
        this.metadataCache = resultSet.getMetaData();
        this.taskResult = taskResult;
    }

    private ResultSetMetaData getMetaData() throws SQLException {
        return metadataCache;
    }

    private ArrayList<Integer> getColumns(NamedParameterPreparedStatement preparedStatement)
            throws SQLException {

        if (columnIndexes == null) {

            synchronized (this) {
                if (columnIndexes == null) {
                    final ArrayList<Integer> list = new ArrayList<>(getMetaData().getColumnCount());

                    for (int columnIndex = 1; columnIndex <= getMetaData().getColumnCount(); columnIndex++) {
                        if (preparedStatement.hasNamedParameter(getMetaData().getColumnName(columnIndex))) {
                            list.add(columnIndex);
                        } else {
                            LOGGER.warn("Column {} not used", getMetaData().getColumnName(columnIndex));
                        }
                    }
                    columnIndexes = list;
                }
            }
        }

        return columnIndexes;
    }

    private List<Object> retrieveData(NamedParameterPreparedStatement preparedStatement) throws SQLException {

        ArrayList<Integer> _columns = getColumns(preparedStatement);

        List<Object> dataList = new ArrayList<>();

        for (final int columnIndex : _columns) {
            dataList.add(retrieveData(columnIndex));
        }

        taskResult.incrementRetrievedRows();

        return dataList;
    }

    private List<Object> retrieveData() throws SQLException {

        List<Object> dataList = new ArrayList<>();

        for (int columnIndex = 1; columnIndex <= getMetaData().getColumnCount(); columnIndex++) {
            dataList.add(retrieveData(columnIndex));
        }

        taskResult.incrementRetrievedRows();

        return dataList;
    }

    private Object retrieveData(int columnIndex) throws SQLException {

        Object value = resultSet.getObject(columnIndex);
        if (resultSet.wasNull()) {
            value = null;
        } else {

            switch (getMetaData().getColumnType(columnIndex)) {
                case Types.CLOB:
                    Clob sourceClob = resultSet.getClob(columnIndex);
                    value = sourceClob.getSubString(1L, (int) sourceClob.length());
                    break;

                    case Types.BLOB:
                        value = resultSet.getBlob(columnIndex);
                        break;
                    case Types.SQLXML:
                        value = resultSet.getSQLXML(columnIndex);
                        break;
                    default:
                }
            }

        return value;
    }

    private void populate(CSVStringBuilder csvBuilder, List<Object> dataList) throws SQLException {

        Iterator<Object> dataIterator = dataList.iterator();
        for (int columnIndex = 1; columnIndex <= getMetaData().getColumnCount(); columnIndex++) {
            final Object value = dataIterator.next();
            String columnName = getMetaData().getColumnName(columnIndex);
            int columnType = getMetaData().getColumnType(columnIndex);
            if (value == null) {
                csvBuilder.append((String) null);
            } else {
                switch (columnType) {
                    case Types.CLOB:
                        Clob clob = (Clob) value;
                        csvBuilder.append(clob.getSubString(0L, (int) clob.length()));
                        break;
                    case Types.SQLXML:
                        SQLXML xml = (SQLXML) value;
                        csvBuilder.append(xml.getString());
                        break;
                    case Types.BOOLEAN:
                        Boolean b = (Boolean) value;
                        // csvBuilder.append(b);
                        break;
                    case Types.DATE:
                        csvBuilder.append((Date) value);
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                        csvBuilder.append((Time) value);
                        break;
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        csvBuilder.append((Timestamp) value);
                        break;
                    case Types.DECIMAL:
                        BigDecimal bigDecimal = (BigDecimal) value;
                        csvBuilder.append(bigDecimal.doubleValue());
                        break;
                    case Types.DOUBLE:
                        csvBuilder.append(((Number) value).doubleValue());
                        break;
                    case Types.REAL:
                    case Types.FLOAT:
                        csvBuilder.append(((Number) value).floatValue());
                        break;
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                        csvBuilder.append(((Number) value).intValue());
                        break;
                    case Types.VARCHAR:
                        csvBuilder.append((String) value);
                        break;
                    case Types.NUMERIC:
                        Number n = (Number) value;
                        int scale = getMetaData().getScale(columnIndex);
                        if (scale == 0) {
                            csvBuilder.append(n.longValue());
                        } else {
                            csvBuilder.append(n.doubleValue());
                        }
                        break;
                    case Types.BIGINT:
                        Number bigInteger = (Number) value;
                        csvBuilder.append(bigInteger.longValue());
                        break;
                    case Types.BLOB:
                    default:
                        csvBuilder.append((String) null);
                }
            }
        }
        csvBuilder.eol();
    }

    private void populateStatement(NamedParameterPreparedStatement preparedStatement, List<Object> dataList)
            throws SQLException {

        Iterator<Object> dataIterator = dataList.iterator();
        for (final int columnIndex : getColumns(preparedStatement)) {
            final Object value = dataIterator.next();
            String columnName = getMetaData().getColumnName(columnIndex);
            int columnType = getMetaData().getColumnType(columnIndex);
            if (value == null) {
                preparedStatement.setNull(columnName, columnType);
            } else {
                switch (columnType) {
                    case Types.CLOB:
                        populateClob(preparedStatement, (Clob) value, columnName);
                        break;
                    case Types.BLOB:
                        populateBlob(preparedStatement, (Blob) value, columnName);
                        break;
                    case Types.SQLXML:
                        populateSqlXml(preparedStatement, (SQLXML) value, columnName);
                        break;
                    default:
                        preparedStatement.setObject(columnName, value);
                }
            }
        }
    }

    private void populateBlob(NamedParameterPreparedStatement preparedStatement, Blob value, String columnName)
            throws SQLException {

        try {
            Blob targetBlob = preparedStatement.getConnection().createBlob();
            OutputStream output = targetBlob.setBinaryStream(1);

            InputStream in = value.getBinaryStream();
            byte[] buf = new byte[1024];
            int len = 0;
            while ((len = in.read(buf)) != -1) {
                output.write(buf, 0, len);
            }

            preparedStatement.setBlob(columnName, targetBlob);
            value.free();
        } catch (IOException e) {
            throw new SQLException("Unable to create Clob object: " + e.getMessage());
        }
    }

    private void populateClob(NamedParameterPreparedStatement preparedStatement, Clob value, String columnName)
            throws SQLException {

        try {
            Clob targetClob = preparedStatement.getConnection().createClob();
            Writer clobWriter = targetClob.setCharacterStream(1);
            clobWriter.write(value.getSubString(0L, (int) value.length()));
            preparedStatement.setClob(columnName, targetClob);
            value.free();
        } catch (IOException e) {
            throw new SQLException("Unable to create Clob object: " + e.getMessage());
        }
    }

    private void populateSqlXml(NamedParameterPreparedStatement preparedStatement, SQLXML value, String columnName)
            throws SQLException {
        SQLXML targetXml = preparedStatement.getConnection().createSQLXML();
        targetXml.setString(value.getString());
        preparedStatement.setSQLXML(columnName, targetXml);
        value.free();
    }

    private boolean isClosed(ResultSet rs) {

        try {
            return rs.isClosed();
        } catch (Error | SQLException e) {
            // ignore: old drivers throws an Error on calling isClosed() method
        }
        return false;
    }

    boolean populateStatement(NamedParameterPreparedStatement preparedStatement) throws SQLException {

        List<Object> dataList;

        synchronized (this) {

            if (isClosed(resultSet) || !resultSet.next()) {
                return false;
            }

            dataList = retrieveData(preparedStatement);
        }

        populateStatement(preparedStatement, dataList);
        return true;
    }

    boolean populate(CSVStringBuilder csvBuilder) throws SQLException {

        List<Object> dataList;

        synchronized (this) {

            if (resultSet.isClosed() || !resultSet.next()) {
                return false;
            }

            dataList = retrieveData();
        }

        populate(csvBuilder, dataList);
        return true;
    }
}
