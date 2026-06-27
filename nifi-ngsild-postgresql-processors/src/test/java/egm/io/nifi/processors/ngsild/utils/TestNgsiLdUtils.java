package egm.io.nifi.processors.ngsild.utils;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.ExportMode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.GENERIC_MEASURE;
import static org.junit.jupiter.api.Assertions.*;

public class TestNgsiLdUtils {

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    @Test
    public void testTemporalEntities() throws IOException {
        String data = loadTestFile("temporalEntities.json");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.EXPANDED);
        assertEquals(2, entities.size());

        List<Attribute> attributes = entities.getFirst().entityAttrs;
        Map<String, List<Attribute>> attributesByObservedAt = attributes.stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        assertEquals(3, attributesByObservedAt.size());
    }

    @Test
    public void verifyIfAttributesAreCompliant() throws IOException {
        String data = loadTestFile("temporalEntities.json");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.EXPANDED);
        assertTrue(entities.stream().allMatch(
            entity -> entity.entityAttrs.stream()
                .allMatch(attributes ->
                    !Objects.equals(attributes.attrName, "nullValue") &&
                        !Objects.equals(attributes.attrName, "nullAttribute")
                )
        ));
    }

    @Test
    public void testEntityWithFlattenObservations() throws IOException {
        String data = loadTestFile("entity-temporal.jsonld");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.FLATTEN);
        assertEquals(1, entities.size());
        Entity entity = entities.getFirst();
        List<Attribute> attributes = entity.entityAttrs;

        assertEquals(13, attributes.size());
        // check all observations have been stored in a generic attribute
        List<String> attributeNames = attributes.stream().map(Attribute::getAttrName).toList();
        assertFalse(attributeNames.contains("faecalcoliform"));
        assertFalse(attributeNames.contains("faecalenterococcus"));
        assertTrue(attributeNames.contains(GENERIC_MEASURE));

        List<Attribute> measureAttributes =
            attributes.stream()
                .filter(attribute -> attribute.getAttrName().equals(GENERIC_MEASURE))
                .toList();
        // the entity under test has four observations
        assertEquals(4, measureAttributes.size());
        Attribute measureAttribute = measureAttributes.getFirst();
        // each observation should have three sub-attributes: parametername, unitcode and datasetId
        assertEquals(3, measureAttribute.subAttrs.size());
        assertTrue(measureAttribute.subAttrs.stream().allMatch(attribute ->
            attribute.getAttrName().equals("parametername") || attribute.getAttrName().equals("unitcode") || attribute.getAttrName().equals("datasetid")
        ));
    }

    @Test
    public void testEntityWithSemiFlattenObservations() throws IOException {
        String data = loadTestFile("entity-temporal-multi-attributes.jsonld");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.SEMI_FLATTEN);
        assertEquals(1, entities.size());
        Entity entity = entities.getFirst();
        List<Attribute> attributes = entity.entityAttrs;

        assertEquals(18, attributes.size());
        // check all observations have been stored in expected format
        List<String> attributeNames = attributes.stream().map(Attribute::getAttrName).toList();
        assertTrue(attributeNames.contains("wateringprogram"));
        assertTrue(attributeNames.contains("simpleattribute"));
        assertFalse(attributeNames.contains("wateringprogram_algorithm:recommendation"));
        assertFalse(attributeNames.contains("wateringprogram_default:observation"));
        assertFalse(attributeNames.contains(GENERIC_MEASURE));

        List<Attribute> wateringPrograms =
            attributes.stream()
                .filter(attribute -> attribute.getAttrName().equals("wateringprogram"))
                .toList();
        // the entity under test has 12 waterProgram observations
        assertEquals(12, wateringPrograms.size());
        Attribute wateringProgram = wateringPrograms.getFirst();
        // each wateringProgram observation should have 2 sub-attributes: unitcode and datasetId
        assertEquals(2, wateringProgram.subAttrs.size());
        assertTrue(wateringProgram.subAttrs.stream().allMatch(attribute ->
                attribute.getAttrName().equals("unitcode") || attribute.getAttrName().equals("datasetid")
        ));

        List<Attribute> simpleAttributes =
            attributes.stream()
                .filter(attribute -> attribute.getAttrName().equals("simpleattribute"))
                .toList();
        // the entity under test has 2 simpleAttribute observations
        assertEquals(2, simpleAttributes.size());
        Attribute simpleAttribute = simpleAttributes.getFirst();
        // each simpleAttribute observation should have 1 sub-attribute (no unitCode)
        assertEquals(1, simpleAttribute.subAttrs.size());
        assertTrue(simpleAttribute.subAttrs.stream().allMatch(attribute ->
                attribute.getAttrName().equals("datasetid")
        ));
    }

    @Test
    public void testParseEntityTypeReturnsStringTypeDirectly() {
        JSONObject entity = new JSONObject();
        entity.put("type", "Distribution");
        assertEquals("Distribution", NgsiLdUtils.parseEntityTypes(entity));
    }

    @Test
    public void testParseEntityTypeJoinsArrayTypesAlphabetically() {
        JSONObject entity = new JSONObject();
        JSONArray types = new JSONArray();
        types.put("ZoneB");
        types.put("ZoneA");
        entity.put("type", types);
        // sorted() → ["ZoneA", "ZoneB"] → joined with "_"
        assertEquals("ZoneA_ZoneB", NgsiLdUtils.parseEntityTypes(entity));
    }

    @Test
    public void testParseEntityScopesReturnsNullWhenAbsent() {
        JSONObject entity = new JSONObject();
        entity.put("type", "Distribution");
        assertNull(NgsiLdUtils.parseEntityScopes(entity));
    }

    @Test
    public void testParseEntityScopesWithStringValueReturnsSetOfOne() {
        JSONObject entity = new JSONObject();
        entity.put("type", "Distribution");
        entity.put("scope", "/S_UseCase/S_Instance");
        assertEquals(Set.of("/S_UseCase/S_Instance"), NgsiLdUtils.parseEntityScopes(entity));
    }

    @Test
    public void testParseEntityScopesWithArrayValueReturnsFullSet() {
        JSONObject entity = new JSONObject();
        entity.put("type", "Distribution");
        JSONArray scopeArray = new JSONArray();
        scopeArray.put("/scope1");
        scopeArray.put("/scope2");
        entity.put("scope", scopeArray);
        assertEquals(Set.of("/scope1", "/scope2"), NgsiLdUtils.parseEntityScopes(entity));
    }

    @Test
    public void testRelationshipAttributeValueIsTheObjectField() {
        String data = "[{" +
            "\"id\": \"urn:ngsi-ld:Thing:001\"," +
            "\"type\": \"Thing\"," +
            "\"linkedTo\": {\"type\": \"Relationship\", \"object\": \"urn:ngsi-ld:Other:123\"}" +
            "}]";
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.EXPANDED);
        Attribute linkedTo = entities.getFirst().getEntityAttrs().stream()
            .filter(a -> a.getAttrName().equals("linkedto"))
            .findFirst().orElseThrow();
        assertEquals("Relationship", linkedTo.getAttrType());
        assertEquals("urn:ngsi-ld:Other:123", linkedTo.getAttrValue());
    }

    @Test
    public void testJsonPropertyAttributeValueIsTheJsonField() {
        String data = "[{" +
            "\"id\": \"urn:ngsi-ld:Thing:001\"," +
            "\"type\": \"Thing\"," +
            "\"metadata\": {\"type\": \"JsonProperty\", \"json\": {\"key\": \"value\", \"num\": 42}}" +
            "}]";
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), ExportMode.EXPANDED);
        Attribute metadata = entities.getFirst().getEntityAttrs().stream()
            .filter(a -> a.getAttrName().equals("metadata"))
            .findFirst().orElseThrow();
        assertEquals("JsonProperty", metadata.getAttrType());
        assertInstanceOf(JSONObject.class, metadata.getAttrValue());
        assertEquals("value", ((JSONObject) metadata.getAttrValue()).getString("key"));
    }
}
