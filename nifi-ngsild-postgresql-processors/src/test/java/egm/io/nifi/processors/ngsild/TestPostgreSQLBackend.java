package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.utils.Entity;
import egm.io.nifi.processors.ngsild.utils.NGSIConstants;
import egm.io.nifi.processors.ngsild.utils.NGSIUtils;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static egm.io.nifi.processors.ngsild.utils.NGSIConstants.GENERIC_MEASURE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPostgreSQLBackend {

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    private final PostgreSQLBackend postgreSQLBackend = new PostgreSQLBackend();

    private final NGSIUtils ngsiUtils = new NGSIUtils();

    @Test
    public void testListOfFieldsWithFlattenedObservations() throws IOException {
        String data = loadTestFile("entity-temporal.jsonld");
        Entity entity = ngsiUtils.parseNgsiLdEntities(new JSONArray(data), true).get(0);

        Map<String, NGSIConstants.POSTGRESQL_COLUMN_TYPES> typedFields =
                postgreSQLBackend.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        assertEquals(26, typedFields.size());
        Set<String> keys = typedFields.keySet();
        assertTrue(keys.contains(GENERIC_MEASURE));
        assertTrue(keys.contains(GENERIC_MEASURE + "_observedat"));
        assertTrue(keys.contains(GENERIC_MEASURE + "_unitcode"));
        assertTrue(keys.contains(GENERIC_MEASURE + "_parametername"));
    }

    @ParameterizedTest
    @CsvSource({"entity-temporal.jsonld, 4", "entity-notification.jsonld, 2"})
    public void testGetValuesForInsertWithFlattenedObservations(String filename, int expectedLines) throws IOException {
        String data = loadTestFile(filename);
        Entity entity = ngsiUtils.parseNgsiLdEntities(new JSONArray(data), true).get(0);

        Map<String, NGSIConstants.POSTGRESQL_COLUMN_TYPES> typedFields =
                postgreSQLBackend.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        List<String> values = postgreSQLBackend.getValuesForInsert(
                entity,
                typedFields,
                Instant.now().toEpochMilli(),
                "urn:ngsi-ld:Dataset:",
                false,
                false,
                true);
        assertEquals(expectedLines, values.size());
    }
}
