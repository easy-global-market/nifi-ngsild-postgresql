package egm.io.nifi.processors.ngsild.utils;

import java.util.regex.Pattern;

public final class NGSICharsets {

    private static final Pattern ENCODEPATTERN = Pattern.compile("[^a-zA-Z0-9\\.\\-]");
    private static final Pattern ENCODEPATTERNSLASH = Pattern.compile("[^a-zA-Z0-9\\.\\-\\/]");

    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private NGSICharsets() {
    } // NGSICharsets

    /**
     * Encodes a string for PostgreSQL. This includes CartoDB. Only lowercase alphanumerics and _ are allowed.
     * @param in
     * @return The encoded string
     */
    public static String encodePostgreSQL(String in) {
        String out = "";

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            int code = c;

            if (code >= 97 && code <= 119) { // a-w --> a-w
                out += c;
            } else if (c == 'x') {
                String next4;

                if (i + 4 < in.length()) {
                    next4 = in.substring(i + 1, i + 5);
                } else {
                    next4 = "WXYZ"; // whatever except a unicode
                } // if else

                if (next4.matches("^[0-9a-fA-F]{4}$")) { // x --> xx
                    out += "xx";
                } else { // x --> x
                    out += c;
                } // if else
            } else if (code == 121 || code == 122) { // yz --> yz
                out += c;
            } else if (code >= 48 && code <= 57) { // 0-9 --> 0-9
                out += c;
            } else if (c == '_') { // _ --> _
                out += c;
            } else if (c == '=') { // = --> xffff
                out += "xffff";
            } else { // --> xUNICODE
                String hex = Integer.toHexString(code);
                out += "x" + ("0000" + hex).substring(hex.length());
            } // else
        } // for

        return out;
    } // encodePostgreSQL

    /**
     * Encodes a string replacing all the non alphanumeric characters by '_' (except by '-' and '.').
     * This should be only called when building a persistence element name, such as table names, file paths, etc.
     *
     * @return The encoded version of the input string.
     */
    public static String encode(String in, boolean deleteSlash, boolean encodeSlash) {
        if (deleteSlash) {
            return ENCODEPATTERN.matcher(in.substring(1)).replaceAll("_");
        } else if (encodeSlash) {
            return ENCODEPATTERN.matcher(in).replaceAll("_");
        } else {
            return ENCODEPATTERNSLASH.matcher(in).replaceAll("_");
        } // if else
    } // encode
} // NGSICharsets
