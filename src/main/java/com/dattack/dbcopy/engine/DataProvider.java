package com.dattack.dbcopy.engine;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dattack.jtoolbox.jdbc.NamedParameterPreparedStatement;

public class DataProvider {

    private final static Logger LOGGER = LoggerFactory.getLogger(DataProvider.class);

    private ResultSet resultSet;
    private volatile ArrayList<Integer> columns;

    public DataProvider(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSet.getMetaData();
    }

    private synchronized ArrayList<Integer> getColumns(NamedParameterPreparedStatement preparedStatement)
            throws SQLException {

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

        return columns;
    }

    boolean populateStatement(NamedParameterPreparedStatement preparedStatement) throws SQLException {

        List<Object> dataList = new ArrayList<>(getColumns(preparedStatement).size());

        synchronized (this) {

            if (resultSet.isClosed() || !resultSet.next()) {
                return false;
            }

            for (final int columnIndex : getColumns(preparedStatement)) {
                final Object value = resultSet.getObject(columnIndex);
                if (resultSet.wasNull()) {
                    dataList.add(null);
                } else {
                    if (getMetaData().getColumnType(columnIndex) == Types.CLOB) {
                        dataList.add(resultSet.getClob(columnIndex));
                    } else {
                        dataList.add(value);
                    }
                }
            }
        }

        Iterator<Object> dataIterator = dataList.iterator();
        for (final int columnIndex : getColumns(preparedStatement)) {
            final Object value = dataIterator.next();
            if (value == null) {
                preparedStatement.setNull(getMetaData().getColumnName(columnIndex),
                        getMetaData().getColumnType(columnIndex));
            } else {
                if (getMetaData().getColumnType(columnIndex) == Types.CLOB) {
                    final Clob sourceClob = (Clob) value;
                    preparedStatement.setObject(getMetaData().getColumnName(columnIndex),
                            sourceClob.getSubString(0L, (int) sourceClob.length()));
                } else {
                    preparedStatement.setObject(getMetaData().getColumnName(columnIndex), value);
                }
            }
        }

        return true;
    }
}
