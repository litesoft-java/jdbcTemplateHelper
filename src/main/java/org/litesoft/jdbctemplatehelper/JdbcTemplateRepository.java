package org.litesoft.jdbctemplatehelper;

import org.litesoft.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;

@SuppressWarnings("unused")
public class JdbcTemplateRepository implements Repository {
    protected JdbcTemplate jdbcTemplate;

    protected JdbcTemplateRepository( JdbcTemplate jdbcTemplate ) {
        this.jdbcTemplate = NotNull.AssertArgument.namedValue( "jdbcTemplate", jdbcTemplate );
    }
}
