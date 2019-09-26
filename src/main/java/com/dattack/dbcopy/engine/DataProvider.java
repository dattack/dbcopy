package com.dattack.dbcopy.engine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;

public class DataProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);

    private ResultSet resultSet;
    private DbCopyTaskResult taskResult;
    private volatile ArrayList<Integer> columns;
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

        if (columns == null) {

            synchronized (this) {
                if (columns == null) {
                    final ArrayList<Integer> list = new ArrayList<>(getMetaData().getColumnCount());

                    for (int columnIndex = 1; columnIndex <= getMetaData().getColumnCount(); columnIndex++) {
                        if (preparedStatement.hasNamedParameter(getMetaData().getColumnName(columnIndex))) {
                            list.add(columnIndex);
                        } else {
                            LOGGER.warn("Column {} not used", getMetaData().getColumnName(columnIndex));
                        }
                    }
                    columns = list;
                }
            }
        }

        return columns;
    }

    private List<Object> retrieveData(NamedParameterPreparedStatement preparedStatement) throws SQLException {

        ArrayList<Integer> _columns = getColumns(preparedStatement);

        List<Object> dataList = new ArrayList<>();

        for (final int columnIndex : _columns) {
            final Object value = resultSet.getObject(columnIndex);
            if (resultSet.wasNull()) {
                dataList.add(null);
            } else {

                switch (getMetaData().getColumnType(columnIndex)) {
                    case Types.CLOB:
                        Clob sourceClob = resultSet.getClob(columnIndex);
                        dataList.add(sourceClob.getSubString(1L, (int) sourceClob.length()));
                        break;
                    case Types.BLOB:
                        dataList.add(resultSet.getBlob(columnIndex));
                        break;
                    case Types.SQLXML:
                        dataList.add(resultSet.getSQLXML(columnIndex));
                        break;
                    default:
                        dataList.add(value);
                }
            }
        }

        taskResult.incrementRetrievedRows();

        return dataList;
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

    boolean populateStatement(NamedParameterPreparedStatement preparedStatement) throws SQLException {

        List<Object> dataList;

        synchronized (this) {

            if (resultSet.isClosed() || !resultSet.next()) {
                return false;
            }

            dataList = retrieveData(preparedStatement);
        }

        populateStatement(preparedStatement, dataList);
        return true;
    }
}
