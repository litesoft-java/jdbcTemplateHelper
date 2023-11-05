package org.litesoft.jdbctemplatehelper.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import lombok.Getter;
import org.litesoft.annotations.NotNull;
import org.litesoft.annotations.Significant;

@Getter
public class ColumnFieldHelper<E, T> {
    private final ColumnTypeProducer<T> resultSetGetter;
    private final InsertColumnRule insertColumnRule;
    private final BiConsumer<E, T> setter;
    private final Function<E, T> getter;
    private final String columnName;

    public ColumnFieldHelper( ColumnTypeProducer<T> resultSetGetter, String columnName, InsertColumnRule insertColumnRule, BiConsumer<E, T> setter, Function<E, T> getter ) {
        this.resultSetGetter = NotNull.AssertArgument.namedValue( "resultSetGetter", resultSetGetter );
        this.insertColumnRule = NotNull.ConstrainTo.valueOr( insertColumnRule, InsertColumnRule.Regular );
        this.columnName = Significant.AssertArgument.namedValue( "columnName", columnName );
        this.setter = NotNull.AssertArgument.namedValue( "setter", setter );
        this.getter = NotNull.AssertArgument.namedValue( "getter", getter );
    }

    public ColumnFieldHelper( ColumnTypeProducer<T> resultSetGetter, String columnName, BiConsumer<E, T> setter, Function<E, T> getter ) {
        this( resultSetGetter, columnName, null, setter, getter );
    }

    public void map( E target, ResultSet rs, int columnIndex )
            throws SQLException {
        T value = resultSetGetter.get( rs, columnIndex );
        setter.accept( target, value );
    }
}

