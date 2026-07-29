package egm.io.nifi.processors.ngsild.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPostgreSQLUtils {

    @Test
    public void encodeReplacesNonAlphanumericCharsWithUnderscore() {
        assertEquals("urn_ngsi_ld_dataset_01", PostgreSQLUtils.encodePostgreSQL("urn:ngsi-ld:Dataset:01"));
    }

    @Test
    public void encodeLowercasesTheResult() {
        assertEquals("someservice", PostgreSQLUtils.encodePostgreSQL("SomeService"));
    }

    @Test
    public void encodePreservesAlphanumericCharacters() {
        assertEquals("abc123", PostgreSQLUtils.encodePostgreSQL("abc123"));
    }

    @Test
    public void encodeHandlesSpacesAndSpecialCharacters() {
        assertEquals("hello_world_", PostgreSQLUtils.encodePostgreSQL("hello world!"));
    }

    @Test
    public void truncateToMaxPgSizeDoesNotShortenStringExactlyAt63Chars() {
        String exactly63 = "a".repeat(63);
        assertEquals(exactly63, PostgreSQLUtils.truncateToMaxPgSize(exactly63));
    }

    @Test
    public void truncateToMaxPgSizeTruncatesStringLongerThan63Chars() {
        String over63 = "a".repeat(80);
        assertEquals(63, PostgreSQLUtils.truncateToMaxPgSize(over63).length());
    }

    @Test
    public void truncateToSizeDoesNotShortenStringWithinLimit() {
        assertEquals("abc", PostgreSQLUtils.truncateToSize("abc", 10));
    }

    @Test
    public void truncateToSizeTruncatesExactlyToGivenLimit() {
        assertEquals("abcde", PostgreSQLUtils.truncateToSize("abcdefgh", 5));
    }

    @Test
    public void truncateToSizeHandlesStringExactlyAtLimit() {
        assertEquals("abcde", PostgreSQLUtils.truncateToSize("abcde", 5));
    }
}
