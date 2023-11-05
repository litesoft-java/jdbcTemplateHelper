package org.litesoft.jdbctemplatehelper;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WhereClauseTest {

    @Test
    void it() {
        WhereClause wc = new WhereClause()
                .add( "(" )
                .add( "(" ).add( "id = oid" ).add( "or" ).add("v != ?", 10).add( ")" )
                .add( "and" )
                .addUnpadded( "x IN (?", 3 )
                .addUnpadded( ",?", 2 )
                .addUnpadded( ",?", 1 )
                .add( ")" )
                .add( ")" );
        assertEquals( " WHERE ((id = oid OR v != ?) AND x IN (?,?,?))", wc.getText());
        List<Object> values = wc.getQuestionMarkValues();
        assertEquals( List.of(10, 3, 2, 1), values );
    }
}