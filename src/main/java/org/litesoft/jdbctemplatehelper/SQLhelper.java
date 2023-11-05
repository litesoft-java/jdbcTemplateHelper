package org.litesoft.jdbctemplatehelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.litesoft.annotations.NotNull;
import org.litesoft.annotations.Nullable;
import org.litesoft.annotations.Positive;
import org.litesoft.annotations.Significant;
import org.litesoft.annotations.SignificantOrEmpty;
import org.litesoft.jdbctemplatehelper.support.ColumnFieldHelper;
import org.litesoft.jdbctemplatehelper.support.ColumnTypeProducer;
import org.litesoft.jdbctemplatehelper.support.InsertColumnRule;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

@SuppressWarnings({"unused", "SpellCheckingInspection"})
public class SQLhelper<T, E> implements RowMapper<E> {
    private final List<ColumnFieldHelper<E, ?>> allCFHs = new ArrayList<>();
    private final List<ColumnFieldHelper<E, ?>> insertCFHs = new ArrayList<>();
    private final Supplier<E> emptyEntityFactory;
    private final JdbcTemplate jdbcTemplate;
    private final String tooManySuffixText;
    private final String tableName;
    private final String name;
    // ID data:
    private final ColumnFieldHelper<E, T> idHelper;
    private final String idColumnName;
    // Version data:
    private final ColumnFieldHelper<E, ?> versionHelper;
    private final String versionColumnName;

    private final String entitySimpleName;

    private final String selectAll;

    public static <T, E> Builder<T, E> builder( @Significant String name, @Nullable Class<T> idClass,
                                                @NotNull Supplier<E> emptyEntityFactory, @Significant String tableName ) {
        return new Builder<>( name, tableName, idClass, emptyEntityFactory );
    }

    private SQLhelper( String name, String tableName, Supplier<E> emptyEntityFactory,
                       List<ColumnFieldHelper<E, ?>> regularCFHs,
                       ColumnFieldHelper<E, T> idHelper, ColumnFieldHelper<E, ?> versionHelper,
                       JdbcTemplate jdbcTemplate, String tooManySuffixText ) {
        this.jdbcTemplate = jdbcTemplate;
        this.tooManySuffixText = tooManySuffixText;
        this.emptyEntityFactory = emptyEntityFactory;
        this.tableName = tableName;
        this.name = name;
        idColumnName = addCFH( this.idHelper = idHelper );
        versionColumnName = addCFH( this.versionHelper = versionHelper );
        for ( ColumnFieldHelper<E, ?> cfh : regularCFHs ) {
            addCFH( cfh );
        }
        entitySimpleName = emptyEntityFactory.get().getClass().getSimpleName();

        selectAll = createSelectAll( allCFHs, tableName );
    }

    private String addCFH( ColumnFieldHelper<E, ?> cfh ) {
        if ( cfh == null ) {
            return null;
        }
        allCFHs.add( cfh );
        if ( cfh.getInsertColumnRule() != InsertColumnRule.No ) {
            insertCFHs.add( cfh );
        }
        return cfh.getColumnName();
    }

    private static <E> String createSelectAll( List<ColumnFieldHelper<E, ?>> allCFHs, String tableName ) {
        StringBuilder sb = new StringBuilder().append( "SELECT" );
        String prefix = "";
        for ( ColumnFieldHelper<?, ?> helper : allCFHs ) {
            sb.append( prefix ).append( ' ' ).append( helper.getColumnName() );
            prefix = ",";
        }
        return sb.append( " FROM " ).append( tableName ).toString();
    }

    public E mapRow( ResultSet rs, int rowNum )
            throws SQLException {
        NotNull.AssertArgument.namedValue( "ResultSet", rs );
        E target = emptyEntityFactory.get();
        for ( int i = 0; i < allCFHs.size(); ) {
            ColumnFieldHelper<E, ?> mapper = allCFHs.get( i );
            mapper.map( target, rs, ++i ); // SQL 1 based!
        }
        return target;
    }

    public void insert( @NotNull E toInsert ) { // C
        NotNull.AssertArgument.namedValue( "toInsert entity", toInsert );
        StringBuilder sbColumns = new StringBuilder().append( "INSERT INTO " ).append( tableName ).append( " (" );
        StringBuilder sbValues = new StringBuilder().append( ") VALUES (" );
        List<Object> values = new ArrayList<>( insertCFHs.size() );
        String prefix = "";
        for ( ColumnFieldHelper<E, ?> cfh : insertCFHs ) {
            Object value = cfh.getGetter().apply( toInsert );
            if ( (value != null) || (cfh.getInsertColumnRule() == InsertColumnRule.Regular) ) {
                values.add( value );
                sbColumns.append( prefix ).append( cfh.getColumnName() );
                sbValues.append( prefix ).append( '?' );
                prefix = ", ";
            }
        }
        String sql = sbColumns.append( sbValues ).append( ')' ).toString();
        if ( values.isEmpty() ) {
            throw new IllegalStateException( "Insert Error -- All columns skipped for " + name + ": " + toInsert );
        }
        if ( 0 == applyUpdate( sql, values.toArray() ) ) {
            throw new IncorrectResultSizeDataAccessException( "Insert Failed for " + name + ": " + toInsert, 1, 0 );
        }
    }

    public boolean updateById( E updated ) { // U
        Object idValue = assertEntityHasNonNullID( "updated entity", updated );
        int versionValue = Integer.MIN_VALUE;
        if ( versionHelper != null ) {
            versionValue = assertVersionIsInteger( entitySimpleName, idColumnName, idValue, versionColumnName,
                                                   versionHelper.getGetter().apply( updated ) );
        }
        StringBuilder sb = new StringBuilder().append( "UPDATE " ).append( tableName ).append( " SET" );
        List<Object> values = new ArrayList<>( allCFHs.size() );
        String prefix = " ";
        for ( ColumnFieldHelper<E, ?> cfh : allCFHs ) {
            if ( cfh != idHelper ) {
                Object fieldValue = (cfh != versionHelper) ?
                                    cfh.getGetter().apply( updated ) :
                                    (versionValue + 1);
                values.add( fieldValue );
                sb.append( prefix ).append( cfh.getColumnName() ).append( " = ?" );
                prefix = ", ";
            }
        }
        sb.append( " WHERE " ).append( idColumnName ).append( " = ?" );
        values.add( idValue );
        if ( versionHelper != null ) {
            sb.append( " AND " ).append( versionColumnName ).append( " = ?" );
            values.add( versionValue );
        }
        String sql = sb.toString();
        return 0 < applyUpdate( sql, values.toArray() );
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public int applyUpdate( String sql, Object... args ) {
        return jdbcTemplate.update( sql, args ); // -> rowsAffected
    }

    public boolean deleteByID( @NotNull T id ) { // D
        assertEntityTypeHasID( "readById" );
        String sql = "DELETE FROM " + tableName + " WHERE " + idColumnName + " = ?";
        return 0 < applyUpdate( sql, NotNull.AssertArgument.namedValue( idColumnName, id ) );
    }

    // Read group

    public E readById( @NotNull T id ) {
        assertEntityTypeHasID( "readById" );
        String sql = selectAll + new WhereClause().add( "id = ?", id ).getText();
        return query1( sql, null, NotNull.AssertArgument.namedValue( idColumnName, id ) );
    }

    @SuppressWarnings({"SqlDialectInspection", "SqlSourceToSinkFlow", "SqlNoDataSourceInspection"})
    public List<T> getIDs( @Nullable T greaterThan, @Nullable WhereClause whereClause, @Positive int limit ) {
        assertEntityTypeHasID( "getIDs" );
        whereClause = NotNull.ConstrainTo.valueOr( whereClause, new WhereClause() );
        if ( greaterThan != null ) {
            whereClause.add( idColumnName + " > ?", greaterThan );
        }
        String sql = "SELECT " + idColumnName + " FROM " + tableName +
                     whereClause.getText() +
                     " ORDER BY " + idColumnName + " ASC LIMIT " +
                     Positive.AssertArgument.namedValue( "limit", limit );

        List<T> ids = jdbcTemplate.query( sql, this::mapId,
                                          toArray( whereClause.getQuestionMarkValues() ) );
        return NotNull.ConstrainTo.valueOr( ids, List.of() );
    }

    private T mapId( ResultSet rs, int rowNum )
            throws SQLException {
        return idHelper.getResultSetGetter().get( rs, 1 );
    }

    public List<E> getEntitiesByIDs( List<T> ids ) {
        assertEntityTypeHasID( "getEntitiesByIDs" );
        ids = nonNullsOnly( ids );
        if ( ids.isEmpty() ) {
            return List.of();
        }
        StringBuilder sb = new StringBuilder().append( selectAll )
                .append( " WHERE " ).append( idColumnName ).append( " IN " );
        char prefix = '(';
        for ( T inID : ids ) {
            sb.append( prefix ).append( inID ); // longs are safe from SQL Injection!
            prefix = ',';
        }
        sb.append( ") ORDER BY " ).append( idColumnName );
        return query( sb.toString() );
    }

    public @Nullable E query1( @NotNull WhereClause whereClause, @Nullable E inserted ) {
        String sqlTemplate = selectAll + NotNull.AssertArgument.namedValue( "whereClause", whereClause ).getText();
        return query1( sqlTemplate, inserted, toArray( whereClause.getQuestionMarkValues() ) );
    }

    public @Nullable E query1( String sql, @Nullable E inserted, Object... args ) {
        List<E> entities = query( sql, args );
        return switch ( entities.size() ) {
            case 1 -> entities.get( 0 );
            case 0 -> zeroQuery1( inserted );
            default -> tooManyQuery1( entities.size(), args );
        };
    }

    public @NotNull List<E> query( @Nullable WhereClause whereClause, String orderBy ) {
        whereClause = WhereClause.deNull( whereClause );
        String sqlTemplate = selectAll + whereClause.getText() + optionalOrderBy( orderBy );
        return query( sqlTemplate, toArray( whereClause.getQuestionMarkValues() ) );
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public @NotNull List<E> query( String sql, Object... args ) {
        return deNull( jdbcTemplate.query( sql, this, args ) );
    }

    private Object[] toArray( List<Object> questionMarkValues ) {
        return NotNull.ConstrainTo.valueOr( questionMarkValues, List.of() ).toArray();
    }

    private String optionalOrderBy( String orderBy ) {
        orderBy = Significant.ConstrainTo.valueOrNull( orderBy );
        return (orderBy == null) ? "" : (" " + orderBy);
    }

    private int assertVersionIsInteger( String entitySimpleName,
                                        String idColumnName, Object idValue,
                                        String versionColumnName, Object versionValue ) {
        if ( versionValue instanceof Integer ) {
            return (Integer)versionValue;
        }
        StringBuilder sb = new StringBuilder()
                .append( "Updated " ).append( entitySimpleName ).append( " with " )
                .append( idColumnName ).append( "=" ).append( idValue )
                .append( " was expected to have a " ).append( versionColumnName )
                .append( " field" ).append( " that is a 'Integer' value, but found" );
        if ( versionValue == null ) {
            sb.append( ": null" );
        } else {
            sb.append( " '" ).append( versionValue.getClass().getSimpleName() )
                    .append( "' of: " ).append( versionValue );
        }
        throw new IllegalStateException( sb.toString() );
    }

    private E zeroQuery1( E inserted ) {
        if ( inserted != null ) {
            throw new IncorrectResultSizeDataAccessException( "No " + name + " Record Stored for: " + inserted, 1, 0 );
        }
        return null;
    }

    private E tooManyQuery1( int entities, Object[] args ) {
        StringBuilder sb = new StringBuilder().append( entities )
                .append( ' ' ).append( name ).append( " Records found" );
        if ( !tooManySuffixText.isEmpty() ) {
            sb.append( ' ' ).append( tooManySuffixText );
        }
        String prefix = ": ";
        for ( Object arg : args ) {
            sb.append( prefix ).append( arg );
            prefix = ", ";
        }
        throw new IncorrectResultSizeDataAccessException( sb.toString(), 1, entities );
    }

    private List<E> deNull( List<E> result ) {
        return (result != null) ? result : List.of();
    }

    private <LT> List<LT> nonNullsOnly( List<LT> values ) {
        if ( values != null ) {
            return values.stream().filter( Objects::nonNull ).toList();
        }
        return List.of();
    }

    private void assertEntityTypeHasID( String what ) {
        if ( idHelper == null ) {
            throw new IllegalStateException( what + " (" + entitySimpleName + ") does NOT have an ID field!" );
        }
    }

    private Object assertEntityHasID( String what, E entity ) {
        assertEntityTypeHasID( what );
        if ( entity == null ) {
            throw new IllegalArgumentException( what + " (" + entitySimpleName + ") was NULL" );
        }
        return idHelper.getGetter().apply( entity );
    }

    @SuppressWarnings("SameParameterValue")
    private Object assertEntityHasNonNullID( String what, E entity ) {
        Object id = assertEntityHasID( what, entity );
        if ( id == null ) {
            throw new IllegalArgumentException( "ID field for " + what + " (" + entitySimpleName +
                                                ") was NULL, but is required: " + entity );
        }
        return id;
    }

    //    // Preserved for Future...
    //    private void assertEntityHasNullID( String what, E entity ) {
    //        Object id = assertEntityHasID( what, entity );
    //        if (id != null) {
    //            throw new IllegalArgumentException( "ID field for " + what + " (" + entitySimpleName +
    //                                                ") was NOT NULL, but NULL required: " + entity );
    //        }
    //    }

    public static class Builder<T, E> {
        private final List<ColumnFieldHelper<E, ?>> cfhs = new ArrayList<>();
        private final Supplier<E> emptyEntityFactory;
        private final Class<T> idClass;
        private final String tableName;
        private final String name;
        private ColumnFieldHelper<E, T> idHelper;
        private ColumnFieldHelper<E, ?> versionHelper;
        private String tooManySuffixText;

        public Builder( @Significant String name, @Significant String tableName,
                        @Nullable Class<T> idClass, @NotNull Supplier<E> emptyEntityFactory ) {
            this.emptyEntityFactory = NotNull.AssertArgument.namedValue( "emptyEntityFactory", emptyEntityFactory );
            this.tableName = Significant.AssertArgument.namedValue( "tableName", tableName );
            this.name = Significant.AssertArgument.namedValue( "name", name );
            this.idClass = idClass;
        }

        public Builder<T, E> addId( ColumnFieldHelper<E, T> newCFM ) {
            idHelper = addSpecial( "ID", idHelper, NotNull.AssertArgument.namedValue( "Id", newCFM ) );
            return this;
        }

        public Builder<T, E> addVersion( ColumnFieldHelper<E, Integer> newCFM ) {
            versionHelper = addSpecial( "version", versionHelper, NotNull.AssertArgument.namedValue( "version", newCFM ) );
            return this;
        }

        public <FT> Builder<T, E> add( ColumnFieldHelper<E, FT> newCFM ) {
            cfhs.add( NotNull.AssertArgument.namedValue( "newCFM", newCFM ) );
            return this;
        }

        public Builder<T, E> addAutoInsertId( ColumnTypeProducer<T> resultSetGetter, String columnName,
                                              BiConsumer<E, T> setter, Function<E, T> getter ) {
            return addId( new ColumnFieldHelper<>( resultSetGetter, columnName, InsertColumnRule.No, setter, getter ) );
        }

        public Builder<T, E> addNonAutoInsertedId( ColumnTypeProducer<T> resultSetGetter, String columnName,
                                                   BiConsumer<E, T> setter, Function<E, T> getter ) {
            return addId( new ColumnFieldHelper<>( resultSetGetter, columnName, setter, getter ) );
        }

        public Builder<T, E> addAutoInsertVersion( ColumnTypeProducer<Integer> resultSetGetter, String columnName,
                                                   BiConsumer<E, Integer> setter, Function<E, Integer> getter ) {
            return addVersion( new ColumnFieldHelper<>( resultSetGetter, columnName, InsertColumnRule.No, setter, getter ) );
        }

        public Builder<T, E> addNonAutoInsertedVersion( ColumnTypeProducer<Integer> resultSetGetter, String columnName,
                                                        BiConsumer<E, Integer> setter, Function<E, Integer> getter ) {
            return addVersion( new ColumnFieldHelper<>( resultSetGetter, columnName, setter, getter ) );
        }

        public <FT> Builder<T, E> add( ColumnTypeProducer<FT> resultSetGetter, String columnName,
                                       BiConsumer<E, FT> setter, Function<E, FT> getter ) {
            return add( resultSetGetter, columnName, null, setter, getter );
        }

        public <FT> Builder<T, E> add( ColumnTypeProducer<FT> resultSetGetter, String columnName, InsertColumnRule insertRule,
                                       BiConsumer<E, FT> setter, Function<E, FT> getter ) {
            cfhs.add( new ColumnFieldHelper<>( resultSetGetter, columnName, insertRule, setter, getter ) );
            return this;
        }

        public Builder<T, E> withQueryOne_tooManySuffixText( @SignificantOrEmpty String tooManySuffixText ) {
            this.tooManySuffixText = tooManySuffixText;
            return this;
        }

        public SQLhelper<T, E> build( @NotNull JdbcTemplate jdbcTemplate ) {
            if ( cfhs.isEmpty() ) {
                throw new IllegalStateException( "No Column (FieldHelper)s registered!" );
            }
            if ( (idClass != null) && (idHelper == null) ) {
                throw new IllegalStateException( "idClass (" + idClass.getSimpleName() + ") indicated, but no 'idHelper' registered" );
            }
            if ( (idClass == null) && (idHelper != null) ) {
                throw new IllegalStateException( "'idHelper' registered, but no id (Class) provided" );
            }
            return new SQLhelper<>( name, tableName, emptyEntityFactory, cfhs, idHelper, versionHelper,
                                    NotNull.AssertArgument.namedValue( "JdbcTemplate", jdbcTemplate ),
                                    Significant.ConstrainTo.valueOrEmpty( tooManySuffixText ) );
        }

        private <FT> ColumnFieldHelper<E, FT> addSpecial( String what, ColumnFieldHelper<E, ?> existing,
                                                          ColumnFieldHelper<E, FT> newCFM ) {
            if ( existing != null ) {
                throw new IllegalStateException( "Attempt to add 2nd " + what + " column," +
                                                 " first '" + existing.getColumnName() + "', " +
                                                 "second: " + newCFM.getColumnName() );
            }
            return newCFM;
        }
    }
}
