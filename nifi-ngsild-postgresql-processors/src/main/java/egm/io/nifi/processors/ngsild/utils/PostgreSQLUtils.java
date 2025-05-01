package egm.io.nifi.processors.ngsild.utils;

import java.util.regex.Pattern;

import static egm.io.nifi.processors.ngsild.model.PostgreSQLConstants.POSTGRESQL_MAX_NAME_LEN;

public class PostgreSQLUtils {

    private static final Pattern ENCODEPOSTGRESQL = Pattern.compile("[^a-zA-Z0-9]");

    /**
     * Encodes a string replacing all the non-alphanumeric characters by '_' (except by '-' and '.').
     * This should be only called when building a persistence element name, such as table names, file paths, etc.
     */
    public static String encodePostgreSQL(String in) {
        return ENCODEPOSTGRESQL.matcher(in).replaceAll("_").toLowerCase();
    }

    public static String truncateToMaxPgSize(String in) {
        if (in.length() > POSTGRESQL_MAX_NAME_LEN + 1)
            return in.substring(0, POSTGRESQL_MAX_NAME_LEN);
        else
            return in;
    }

    public static String truncateToSize(String in, int size) {
        if (in.length() > size + 1)
            return in.substring(0, size);
        else
            return in;
    }
}
