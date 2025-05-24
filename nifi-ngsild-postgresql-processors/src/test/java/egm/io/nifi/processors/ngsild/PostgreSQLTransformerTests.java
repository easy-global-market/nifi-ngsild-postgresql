package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.utils.NgsiLdUtils;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.*;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.GENERIC_MEASURE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class PostgreSQLTransformerTests {

    private final PostgreSQLTransformer pgTransformer = new PostgreSQLTransformer();

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    @Test
    public void testBuildSchemaNameFromTenant() throws Exception {
        String tenantName = "someService";
        String builtSchemaName = pgTransformer.buildSchemaName(tenantName);
        assertEquals("someservice", builtSchemaName);
    }

    @Test
    public void testBuildSchemaNameFailsIfAbove63() {
        String tenantName = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongService";
        assertThrows(Exception.class, () -> pgTransformer.buildSchemaName(tenantName));
    }

    @Test
    public void testBuildTableNameFailsIfAbove63() {
        String entityType = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongType";
        Entity entity = new Entity("urn:1", entityType, null, null);
        assertThrows(Exception.class, () -> pgTransformer.buildTableName(entity, null));
    }

    @Test
    public void testListOfFieldsFindsAllTheFields() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> expList = Arrays.asList("entityId", "entityType", "recvTime", "someattr_urn_ngsi_ld_dataset_01", "someattr_urn_ngsi_ld_dataset_01_observedat");
        Set<String> expectedListOfFields = new HashSet<>(expList);

        assertEquals(expectedListOfFields, listOfFields.keySet());

    }

    @Test
    public void testScopeAttributeIsInListOfFieldsWithCorrectValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", Set.of("S_UseCase/S_Instance"), entityAttrs);

            Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
            Set<String> expectedListOfFields = Set.of("entityId", "entityType", "scopes", "recvTime", "someattr_urn_ngsi_ld_dataset_01", "someattr_urn_ngsi_ld_dataset_01_observedat");
            assertEquals(expectedListOfFields, listOfFields.keySet());

            List<String> valuesForInsert = pgTransformer.getValuesForInsert(entity, listOfFields, 1562561734983L, "", false, false, false);
            assertTrue(valuesForInsert.get(0).contains("'{S_UseCase/S_Instance}'"));
    }

    @Test
    public void testValuesForInsertGeneratesCorrectStatement() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);
        long creationTime = 1562561734983L;

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> valuesForInsert = pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, false);
        List<String> expectedValuesForInsert = List.of("('someId','someType','2019-07-08T04:55:34.983Z',12.0,'2023-02-16T00:00:00Z')");
        assertEquals(expectedValuesForInsert, valuesForInsert);
    }

    @Test
    public void testIgnoredAttributesForTopLevelAttribute() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        entityAttrs.add(new Attribute("ignoredAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);
        Set<String> ignoredAttributes = new HashSet<>(Arrays.asList("ignoredAttr", "anotherIgnoredAttr"));
        long creationTime = 1562561734983L;

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, ignoredAttributes);
        List<String> valuesForInsert =
            pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, false);

        assertTrue(listOfFields.keySet().stream().noneMatch(key -> key.contains("ignoredattr")));
        // values for ignored attribute should not be in the values for insert
        assertEquals(5, valuesForInsert.get(0).split(",").length);
    }

    @Test
    public void testIgnoredAttributesForSubAttribute() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Attribute subAttribute =
            new Attribute("ignoredSubAttr", "Property", null, null, null, null, 12.0, false, new ArrayList<>());
        entityAttrs.add(new Attribute("anotherAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, true, Collections.singletonList(subAttribute)));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);
        Set<String> ignoredAttributes = new HashSet<>(Arrays.asList("ignoredAttr", "ignoredSubAttr"));
        long creationTime = 1562561734983L;

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, ignoredAttributes);
        List<String> valuesForInsert =
            pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, false);

        assertTrue(listOfFields.keySet().stream().noneMatch(key -> key.contains("ignoredsubattr")));
        // values for ignored sub-attribute should not be in the values for insert
        assertEquals(7, valuesForInsert.get(0).split(",").length);
    }

    @Test
    public void testUpdatedListOfTypedFieldShouldDetectAttributeTypeChange() throws Exception {
        ResultSet resultSetMock = Mockito.mock(ResultSet.class);
        when(resultSetMock.getString(1)).thenReturn("temperature");
        when(resultSetMock.getString(2)).thenReturn("numeric");
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = new TreeMap<>();
        listOfFields.put("temperature", PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.TEXT);

        listOfFields = pgTransformer.getUpdatedListOfTypedFields(resultSetMock, listOfFields);

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.NUMERIC, listOfFields.get("temperature"));
        assertNotEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.TEXT, listOfFields.get("temperature"));
    }

    @Test
    public void testListOfFieldsWithFlattenedObservations() throws IOException {
        String data = loadTestFile("entity-temporal.jsonld");
        Entity entity = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), true).get(0);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> typedFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        assertEquals(27, typedFields.size());
        Set<String> keys = typedFields.keySet();
        assertTrue(keys.contains(GENERIC_MEASURE));
        assertTrue(keys.contains(GENERIC_MEASURE + "_observedat"));
        assertTrue(keys.contains(GENERIC_MEASURE + "_unitcode"));
        assertTrue(keys.contains(GENERIC_MEASURE + "_parametername"));
        assertTrue(keys.contains(GENERIC_MEASURE + "_datasetid"));
    }

    @ParameterizedTest
    @CsvSource({"entity-temporal.jsonld, 4", "entity-notification.jsonld, 2"})
    public void testGetValuesForInsertWithFlattenedObservations(String filename, int expectedLines) throws IOException {
        String data = loadTestFile(filename);
        Entity entity = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), true).get(0);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> typedFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
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
