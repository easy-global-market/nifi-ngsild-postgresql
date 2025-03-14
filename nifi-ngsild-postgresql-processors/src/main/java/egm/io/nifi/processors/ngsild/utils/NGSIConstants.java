package egm.io.nifi.processors.ngsild.utils;

public final class NGSIConstants {

    // Common fields for sinks
    public static final String RECV_TIME = "recvTime";
    public static final String ENTITY_ID = "entityId";
    public static final String ENTITY_TYPE = "entityType";
    public static final String ENTITY_SCOPES = "scopes";

    // NGSIPostgreSQLSink specific constants
    // http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    public static final int POSTGRESQL_MAX_NAME_LEN = 63;
    public static final String OLD_CONCATENATOR = "_";
    public static final String OBSERVED_AT = "observedAt";
    public static final String CREATED_AT = "createdAt";
    public static final String MODIFIED_AT = "modifiedAt";
    public static final String GENERIC_MEASURE = "measure";

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private NGSIConstants() {
    } // NGSIConstants

    public enum POSTGRESQL_COLUMN_TYPES {
        TEXT,
        TIMESTAMPTZ,
        TIMETZ,
        DATE,
        NUMERIC,
        GEOMETRY,
        JSONB,
        ARRAY;

        public String getPostgresType() {
            if (this == POSTGRESQL_COLUMN_TYPES.ARRAY) {
                return "text[]";
            }
            return this.name();
        }
    }


} // NGSIConstants
