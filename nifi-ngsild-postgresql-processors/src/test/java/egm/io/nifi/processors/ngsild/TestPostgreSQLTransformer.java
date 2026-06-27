package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.ExportMode;
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

public class TestPostgreSQLTransformer {

    private final PostgreSQLTransformer pgTransformer = new PostgreSQLTransformer();

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    @Test
    public void testBuildSchemaNameFromTenant() {
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
        List<String> expList = Arrays.asList("entityId", "entityType", "recvTime", "someattr_urn_ngsi_ld_dataset_01", "observedat");
        Set<String> expectedListOfFields = new HashSet<>(expList);

        assertEquals(expectedListOfFields, listOfFields.keySet());

    }

    @Test
    public void testScopeAttributeIsInListOfFieldsWithCorrectValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", Set.of("S_UseCase/S_Instance"), entityAttrs);

            Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
            Set<String> expectedListOfFields = Set.of("entityId", "entityType", "scopes", "recvTime", "someattr_urn_ngsi_ld_dataset_01", "observedat");
            assertEquals(expectedListOfFields, listOfFields.keySet());

            List<String> valuesForInsert = pgTransformer.getValuesForInsert(entity, listOfFields, 1562561734983L, "", false, false, ExportMode.EXPANDED);
            assertTrue(valuesForInsert.getFirst().contains("'{S_UseCase/S_Instance}'"));
    }

    @Test
    public void testValuesForInsertGeneratesCorrectStatement() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);
        long creationTime = 1562561734983L;

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields = pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> valuesForInsert = pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, ExportMode.EXPANDED);
        List<String> expectedValuesForInsert = List.of("('someId','someType','2023-02-16T00:00:00Z','2019-07-08T04:55:34.983Z',12.0)");
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
            pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, ExportMode.EXPANDED);

        assertTrue(listOfFields.keySet().stream().noneMatch(key -> key.contains("ignoredattr")));
        // values for ignored attribute should not be in the values for insert
        assertEquals(5, valuesForInsert.getFirst().split(",").length);
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
            pgTransformer.getValuesForInsert(entity, listOfFields, creationTime, "", false, true, ExportMode.EXPANDED);

        assertTrue(listOfFields.keySet().stream().noneMatch(key -> key.contains("ignoredsubattr")));
        // values for ignored sub-attribute should not be in the values for insert
        assertEquals(6, valuesForInsert.getFirst().split(",").length);
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
        Entity entity = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.FLATTEN).getFirst();

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
        Entity entity = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.FLATTEN).getFirst();

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> typedFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
            entity,
            typedFields,
            Instant.now().toEpochMilli(),
            "urn:ngsi-ld:Dataset:",
            false,
            false,
                ExportMode.FLATTEN);
        assertEquals(expectedLines, values.size());
    }

    @Test
    public void testListOfFieldsInfersDateTypeForIsoDateValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("expirydate", "Property", "", "2023-02-16T00:00:00Z", null, null, "2023-02-16", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.DATE, listOfFields.get("expirydate"));
    }

    @Test
    public void testListOfFieldsInfersTimetzTypeForIsoTimeValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("mytime", "Property", "", "2023-02-16T00:00:00Z", null, null, "10:30:00+00:00", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.TIMETZ, listOfFields.get("mytime"));
    }

    @Test
    public void testListOfFieldsInfersTimestamptzTypeForIsoDateTimeValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("updatedat", "Property", "", "2023-02-16T00:00:00Z", null, null, "2023-02-16T10:30:00Z", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ, listOfFields.get("updatedat"));
    }

    @Test
    public void testListOfFieldsInfersTextTypeForPlainStringValue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("label", "Property", "", "2023-02-16T00:00:00Z", null, null, "some text value", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.TEXT, listOfFields.get("label"));
    }

    @Test
    public void testListOfFieldsInfersJsonbTypeForJsonPropertyAttribute() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("metadata", "JsonProperty", "", "2023-02-16T00:00:00Z", null, null, "{\"key\":\"value\"}", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());

        assertEquals(PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES.JSONB, listOfFields.get("metadata"));
    }

    @Test
    public void testListOfFieldsTruncatesUuidDatasetIdToFirstEightChars() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute(
            "someAttr", "Property",
            "urn:ngsi-ld:Dataset:550e8400-e29b-41d4-a716-446655440000",
            "2023-02-16T00:00:00Z",
            null, null, 12.0, false, new ArrayList<>()
        ));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());

        assertTrue(listOfFields.containsKey("someattr_550e8400"),
            "Column name should use only the first 8 chars of the UUID datasetId");
        assertFalse(listOfFields.keySet().stream().anyMatch(k -> k.startsWith("someattr_550e8400_")),
            "Column name should not contain the full UUID");
    }

    @Test
    public void testListOfFieldsKeepsNonUuidDatasetIdAsIs() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute(
            "someAttr", "Property",
            "urn:ngsi-ld:Dataset:k63_0to1meter",
            "2023-02-16T00:00:00Z",
            null, null, 12.0, false, new ArrayList<>()
        ));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());

        assertTrue(listOfFields.containsKey("someattr_k63_0to1meter"),
            "Non-UUID datasetId should be kept without truncation");
    }

    @Test
    public void testGetValuesForInsertWithSemiFlattenObservations() throws IOException {
        String data = loadTestFile("entity-temporal-multi-attributes.jsonld");
        Entity entity = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.SEMI_FLATTEN).getFirst();

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> typedFields =
            pgTransformer.listOfFields(entity, "urn:ngsi-ld:Dataset:", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
            entity, typedFields, Instant.now().toEpochMilli(), "urn:ngsi-ld:Dataset:", false, true, ExportMode.SEMI_FLATTEN
        );

        // 4 datasetIds × 3 timestamps = 12 wateringProgram rows + 2 simpleAttribute rows = 14 rows total
        // (static attrs with no observedAt are ignored since ignoreEmptyObservedAt=true)
        assertEquals(14, values.size());
    }

    @Test
    public void testGetValuesForInsertReturnsEmptyListWhenAllAttributesHaveNoObservedAtAndIgnoreIsTrue() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "", "", null, null, "value", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
            entity, listOfFields, Instant.now().toEpochMilli(), "", false, true, ExportMode.EXPANDED
        );

        assertTrue(values.isEmpty());
    }

    @Test
    public void testInsertQueryReturnsFakeStatementWhenNoValuesToInsert() {
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("someAttr", "Property", "", "", null, null, "value", false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        String sql = pgTransformer.insertQuery(
            entity, Instant.now().toEpochMilli(), "public", "sometype", listOfFields, "", false, true, ExportMode.EXPANDED
        );

        assertEquals("select 1;", sql);
    }

    // P6 continued — referential entity behavior (all attributes have no observedAt)

    @Test
    public void testGetValuesForInsertProducesOneRowForReferentialEntityWhenIgnoreEmptyObservedAtIsFalse() {
        // "referential entity" = all attributes have no observedAt (e.g. a school record)
        // The fast-return guard (observedTimestamps.isEmpty()) does NOT trigger here because
        // observedTimestamps = [""] (one empty-string key), not an empty list
        ArrayList<Attribute> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attribute("name", "Property", "", "", null, null, "Ecole de la Paix", false, new ArrayList<>()));
        entityAttrs.add(new Attribute("address", "Property", "", "", null, null, "123 Rue de la Paix", false, new ArrayList<>()));
        Entity entity = new Entity("urn:ngsi-ld:School:001", "School", null, entityAttrs);

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
            entity, listOfFields, Instant.now().toEpochMilli(), "", false, false, ExportMode.EXPANDED
        );

        assertEquals(1, values.size(),
            "Referential entity must produce one row when ignoreEmptyObservedAt is false");
    }

    @Test
    public void testGetValuesForInsertReturnsEmptyListOnlyForEntityWithZeroAttributes() {
        // The fast-return guard fires only when the entity truly has no attributes at all
        Entity entity = new Entity("someId", "someType", null, new ArrayList<>());

        Map<String, PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES> listOfFields =
            pgTransformer.listOfFields(entity, "", false, Collections.emptySet());
        List<String> values = pgTransformer.getValuesForInsert(
            entity, listOfFields, Instant.now().toEpochMilli(), "", false, false, ExportMode.EXPANDED
        );

        assertTrue(values.isEmpty(), "Entity with zero attributes should produce no rows");
    }
}
