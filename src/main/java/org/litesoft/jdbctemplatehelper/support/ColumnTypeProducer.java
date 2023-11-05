package org.litesoft.jdbctemplatehelper.support;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnTypeProducer<T> {
    T get( ResultSet rs, int columnIndex )
            throws SQLException;
}
