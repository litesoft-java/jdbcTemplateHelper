package org.litesoft.jdbctemplatehelper;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.litesoft.annotations.NotNull;

public class WhereClause {
    public static final WhereClause EMPTY = new WhereClause();

    public static final Entry AND = new SimpleEntry( "AND", true );
    public static final Entry OR = new SimpleEntry( "OR", true );
    public static final Entry PAREN_OPEN = new SimpleEntry( "(", false ) {
        @Override
        protected boolean shouldLeftPad( @NotNull Entry previousEntry ) {
            return previousEntry != PAREN_OPEN;
        }
    };

    public static final Entry PAREN_CLOSE = new SimpleEntry( ")", true ) {
        @Override
        protected boolean shouldLeftPad( Entry previousEntry ) {
            return false;
        }
    };

    public interface Entry {
        @SuppressWarnings("unused")
        String getText();

        default boolean hasQuestionMarkValue() {
            return false;
        }

        default EntryWithQuestionMarkValue toQuestionMarkEntry() {
            return null;
        }

        boolean wantsPadding();

        void appendTo( StringBuilder sb, Entry previousEntry );
    }

    public interface EntryWithQuestionMarkValue extends Entry {
        Object getValue();

        @Override
        default boolean hasQuestionMarkValue() {
            return true;
        }

        @Override
        default EntryWithQuestionMarkValue toQuestionMarkEntry() {
            return this;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public WhereClause add( Entry entry ) {
        if ( entry != null ) {
            entries.add( entry );
        }
        return this;
    }

    public WhereClause add( String text ) {
        if ( "AND".equalsIgnoreCase( text ) ) {
            return add( AND );
        }
        if ( "OR".equalsIgnoreCase( text ) ) {
            return add( OR );
        }
        if ( "(".equals( text ) ) {
            return add( PAREN_OPEN );
        }
        if ( ")".equals( text ) ) {
            return add( PAREN_CLOSE );
        }
        return addPadding( true, text );
    }

    @SuppressWarnings("unused")
    public WhereClause addUnpadded( String text ) {
        return addPadding( false, text );
    }

    public WhereClause add( String text, Object questionMarkValue ) {
        return addPadding( true, text, questionMarkValue );
    }

    public WhereClause addUnpadded( String text, Object questionMarkValue ) {
        return addPadding( false, text, questionMarkValue );
    }

    public List<Object> getQuestionMarkValues() {
        List<Object> values = new ArrayList<>();
        for ( Entry entry : entries ) {
            if ( entry.hasQuestionMarkValue() ) {
                values.add( entry.toQuestionMarkEntry().getValue() );
            }
        }
        return values;
    }

    public String getText() {
        if ( entries.isEmpty() ) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Entry lastEntry = null;
        for ( Entry entry : entries ) {
            entry.appendTo( sb, lastEntry );
            lastEntry = entry;
        }
        return " WHERE " + sb;
    }

    @Override
    public String toString() {
        return "WhereClause[" + entries + ']';
    }

    private WhereClause addPadding( boolean pad, String text ) {
        return add( new SimpleEntry( text, pad ) );
    }

    private WhereClause addPadding( boolean pad, String text, Object questionMarkValue ) {
        return add( new QuestionMarkValueEntry( text, pad, questionMarkValue ) );
    }

    @Getter
    private static class QuestionMarkValueEntry extends SimpleEntry implements EntryWithQuestionMarkValue {
        private final Object value;

        public QuestionMarkValueEntry( String text, boolean wantsPadding, Object value ) {
            super( text, wantsPadding );
            this.value = value;
        }

        @Override
        public String toString() {
            return getText() + "{?:" + value + "}";
        }
    }

    @RequiredArgsConstructor
    private static class SimpleEntry implements Entry {
        @Getter
        private final String text;
        private final boolean wantsPadding;

        @Override
        public boolean wantsPadding() {
            return wantsPadding;
        }

        @Override
        public String toString() {
            return text;
        }

        @Override
        public void appendTo( StringBuilder sb, Entry previousEntry ) {
            if ( (previousEntry != null) && shouldLeftPad( previousEntry ) ) {
                sb.append( ' ' );
            }
            sb.append( text );
        }

        protected boolean shouldLeftPad( @NotNull Entry previousEntry ) {
            return (previousEntry != PAREN_OPEN) &&
                   (wantsPadding() || previousEntry.wantsPadding());
        }
    }

    public static WhereClause deNull( WhereClause whereClause ) {
        return (whereClause != null) ? whereClause : EMPTY;
    }
}
