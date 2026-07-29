package egm.io.nifi.processors.ngsild.utils;

import java.util.regex.Pattern;

import static egm.io.nifi.processors.ngsild.model.PostgreSQLConstants.POSTGRESQL_MAX_NAME_LEN;

public class PostgreSQLUtils {

    private static final Pattern ENCODEPOSTGRESQL = Pattern.compile("[^a-zA-Z0-9]");

    /**
     * Encodes a string for use as a PostgreSQL identifier by replacing all
     * non-alphanumeric characters with '_' and lowercasing the result.
     */
    public static String encodePostgreSQL(String in) {
        return ENCODEPOSTGRESQL.matcher(in).replaceAll("_").toLowerCase();
    }

    public static String truncateToMaxPgSize(String in) {
        return truncate(in, POSTGRESQL_MAX_NAME_LEN);
    }

    public static String truncateToSize(String in, int size) {
        return truncate(in, size);
    }

    private static String truncate(String in, int limit) {
        return in.length() > limit ? in.substring(0, limit) : in;
    }
}
