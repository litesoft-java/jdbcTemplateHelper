package org.litesoft.jdbctemplatehelper;

@SuppressWarnings("unused")
public class EntityUpdateException extends RuntimeException {
    public EntityUpdateException( String message ) {
        super( message );
    }

    public EntityUpdateException( String message, Throwable cause ) {
        super( message, cause );
    }

    public EntityUpdateException( Throwable cause ) {
        super( cause );
    }
}
