package egm.io.nifi.processors.ngsild.model;

public final class PostgreSQLConstants {

    // http://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    public static final int POSTGRESQL_MAX_NAME_LEN = 63;
    public static final String OLD_CONCATENATOR = "_";

    public static final String RECV_TIME = "recvTime";
    public static final String ENTITY_ID = "entityId";
    public static final String ENTITY_TYPE = "entityType";
    public static final String ENTITY_SCOPES = "scopes";
}
