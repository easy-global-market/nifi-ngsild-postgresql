package egm.io.nifi.processors.ngsild.utils;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.GENERIC_MEASURE;
import static org.junit.jupiter.api.Assertions.*;

public class NgsiLdUtilsTests {

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    @Test
    public void testTemporalEntities() throws IOException {
        String data = loadTestFile("temporalEntities.json");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), false);
        assertEquals(2, entities.size());

        List<Attribute> attributes = entities.get(0).entityAttrs;
        Map<String, List<Attribute>> attributesByObservedAt = attributes.stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        assertEquals(3, attributesByObservedAt.size());
    }

    @Test
    public void verifyIfAttributesAreCompliant() throws IOException {
        String data = loadTestFile("temporalEntities.json");
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), false);
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
        List<Entity> entities = NgsiLdUtils.parseNgsiLdEntities(new JSONArray(data), true);
        assertEquals(1, entities.size());
        Entity entity = entities.get(0);
        List<Attribute> attributes = entity.entityAttrs;

        assertEquals(13, attributes.size());
        // check all observations have been stored in a generic attribute
        List<String> attributeNames = attributes.stream().map(Attribute::getAttrName).collect(Collectors.toList());
        assertFalse(attributeNames.contains("faecalcoliform"));
        assertFalse(attributeNames.contains("faecalenterococcus"));
        assertTrue(attributeNames.contains(GENERIC_MEASURE));

        List<Attribute> measureAttributes =
            attributes.stream()
                .filter(attribute -> attribute.getAttrName().equals(GENERIC_MEASURE))
                .collect(Collectors.toList());
        // the entity under test has four observations
        assertEquals(4, measureAttributes.size());
        Attribute measureAttribute = measureAttributes.get(0);
        // each observation should have three sub-attributes: parametername, unitcode and datasetId
        assertEquals(3, measureAttribute.subAttrs.size());
        assertTrue(measureAttribute.subAttrs.stream().allMatch(attribute ->
            attribute.getAttrName().equals("parametername") || attribute.getAttrName().equals("unitcode") || attribute.getAttrName().equals("datasetid")
        ));
    }
}
