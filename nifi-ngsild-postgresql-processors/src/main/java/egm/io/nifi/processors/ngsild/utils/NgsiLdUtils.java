package egm.io.nifi.processors.ngsild.utils;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.Event;
import egm.io.nifi.processors.ngsild.model.ExportMode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.*;

public class NgsiLdUtils {

    private static final Logger logger = LoggerFactory.getLogger(NgsiLdUtils.class);

    private static final List<String> IGNORED_KEYS_ON_ATTRIBUTES =
        List.of("type", "value", "object", "json", "datasetId", "createdAt", "modifiedAt", "instanceId", "observedAt");
    private static final List<String> IGNORED_KEYS_ON_ENTITIES =
        List.of("id", "type", "scope", "@context", "createdAt", "modifiedAt");

    public static Event getEventFromFlowFile(FlowFile flowFile, ExportMode exportMode, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);

        JSONArray content = new JSONArray(flowFileContent);
        List<Entity> entities = parseNgsiLdEntities(content, exportMode);

        return new Event(flowFile.getEntryDate(), entities);
    }

    public static List<Entity> parseNgsiLdEntities(JSONArray content, ExportMode exportMode) {
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < content.length(); i++) {
            JSONObject temporalEntity = content.getJSONObject(i);
            String entityId = temporalEntity.getString("id");
            String entityType = parseEntityTypes(temporalEntity);
            Set<String> scopes = parseEntityScopes(temporalEntity);
            logger.debug("Dealing with entity {} of type(s) {} in scope(s) {}", entityId, entityType, scopes);

            List<Attribute> attributes = new ArrayList<>();
            Iterator<String> keys = temporalEntity.keys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (!IGNORED_KEYS_ON_ENTITIES.contains(key)) {
                    forEachAttributeValue(temporalEntity.get(key), key, (attrKey, attrValue) -> {
                        Attribute attribute = parseNgsiLdAttribute(attrKey, attrValue, exportMode);
                        addAttributeIfValid(attributes, attribute);
                    });
                }
            }

            entities.add(new Entity(entityId, entityType, scopes, attributes));
        }
        return entities;
    }

    protected static Set<String> parseEntityScopes(JSONObject temporalEntity) {
        if (!temporalEntity.has("scope")) {
            return null;
        } else if (temporalEntity.get("scope") instanceof JSONArray) {
            return temporalEntity.getJSONArray("scope")
                .toList().stream()
                .map(scope -> (String) scope)
                .collect(Collectors.toSet());
        } else {
            return Set.of(temporalEntity.getString("scope"));
        }
    }

    protected static String parseEntityTypes(JSONObject temporalEntity) {
        if (temporalEntity.get("type") instanceof JSONArray) {
            return temporalEntity.getJSONArray("type")
                .toList()
                .stream().map(type -> (String) type)
                .sorted()
                .collect(Collectors.joining("_"));
        } else {
            return temporalEntity.getString("type");
        }
    }

    private static Attribute parseNgsiLdAttribute(String key, JSONObject value, ExportMode exportMode) {
        // When exporting the temporal history of an entity, the value of an attribute can be an empty array - as per the specification -
        // if it has no history in the specified time range.
        // In this case, some flow file can give entity that contains attributes with only null values so attribute type can be set to null
        String attrType = value.optString("type");
        String datasetId = value.optString("datasetId");
        String observedAt = value.optString("observedAt");
        String createdAt = value.optString("createdAt");
        String modifiedAt = value.optString("modifiedAt");
        Object attrValue;
        ArrayList<Attribute> subAttributes = new ArrayList<>();

        boolean isFlatten = ExportMode.FLATTEN.equals(exportMode);
        boolean isSemiFlatten = ExportMode.SEMI_FLATTEN.equals(exportMode);

        if (ATTR_TYPE_RELATIONSHIP.equals(attrType)) {
            attrValue = value.get("object").toString();
        } else if (ATTR_TYPE_PROPERTY.equals(attrType)) {
            attrValue = value.opt("value");
        } else if (ATTR_TYPE_GEO_PROPERTY.equals(attrType)) {
            attrValue = value;
        } else if (ATTR_TYPE_JSON_PROPERTY.equals(attrType)) {
            attrValue = value.getJSONObject("json");
        } else if ("".equals(attrType)) {
            attrType = null;
            attrValue = null;
        } else {
            logger.warn("Unrecognized attribute type: {}", attrType);
            return null;
        }

        Iterator<String> keysOneLevel = value.keys();
        while (keysOneLevel.hasNext()) {
            String keyOne = keysOneLevel.next();
            if (ATTR_TYPE_PROPERTY.equals(attrType) && "unitCode".equals(keyOne)) {
                if (value.get(keyOne) instanceof String)
                    subAttributes.add(new Attribute(keyOne.toLowerCase(), ATTR_TYPE_PROPERTY, "", "", "", "", value.getString(keyOne), false, null));
            } else if (keyOne.equals("entity") || keyOne.equals("RelationshipDetails")) {
                JSONObject relation = value.getJSONObject(keyOne);
                relation.remove("id");
                relation.remove("type");
                relation.remove("scope");

                for (String relationKey : relation.keySet()) {
                    forEachAttributeValue(relation.get(relationKey), relationKey, (rKey, rValue) -> {
                        Attribute subAttribute = parseNgsiLdSubAttribute(rKey, rValue);
                        addAttributeIfValid(subAttributes, subAttribute);
                    });
                }
            } else if (!IGNORED_KEYS_ON_ATTRIBUTES.contains(keyOne)) {
                forEachAttributeValue(value.get(keyOne), keyOne, (subKey, subValue) -> {
                    Attribute subAttribute = parseNgsiLdSubAttribute(subKey, subValue);
                    addAttributeIfValid(subAttributes, subAttribute);
                });
            }
        }

        if ((isFlatten || isSemiFlatten) && !observedAt.isEmpty()) {
            if (datasetId.isEmpty()) {
                datasetId = "default";
            }
            Attribute parameterDatasetId = new Attribute(
                "datasetid", ATTR_TYPE_PROPERTY, "", "", "", "", datasetId.toLowerCase(), false, null
            );
            subAttributes.add(parameterDatasetId);
            if (isFlatten) {
                Attribute parameterName = new Attribute(
                    "parametername", ATTR_TYPE_PROPERTY, "", "", "", "", key.toLowerCase(), false, null
                );
                subAttributes.add(parameterName);
                return new Attribute(GENERIC_MEASURE, attrType, "", observedAt, createdAt, modifiedAt, attrValue, true, subAttributes);
            } else {
                return new Attribute(normalizeAttributeName(key), attrType, "", observedAt, createdAt, modifiedAt, attrValue, true, subAttributes);
            }
        } else {
            return new Attribute(normalizeAttributeName(key), attrType, datasetId, observedAt, createdAt, modifiedAt, attrValue, !subAttributes.isEmpty(), subAttributes);
        }
    }

    private static Attribute parseNgsiLdSubAttribute(String key, JSONObject value) {
        String subAttrType = value.get("type").toString();
        Object subAttrValue = "";
        if (ATTR_TYPE_RELATIONSHIP.equals(subAttrType)) {
            subAttrValue = value.get("object").toString();
        } else if (ATTR_TYPE_PROPERTY.equals(subAttrType)) {
            subAttrValue = value.get("value");
        } else if (ATTR_TYPE_GEO_PROPERTY.equals(subAttrType)) {
            subAttrValue = value.get("value").toString();
        } else if (ATTR_TYPE_JSON_PROPERTY.equals(subAttrType)) {
            subAttrValue = value.get("json").toString();
        }

        return new Attribute(normalizeAttributeName(key), subAttrType, "", "", "", "", subAttrValue, false, null);
    }

    @FunctionalInterface
    private interface AttributeValueConsumer {
        void accept(String key, JSONObject value);
    }

    private static void forEachAttributeValue(Object node, String key, AttributeValueConsumer consumer) {
        if (node instanceof JSONArray array) {
            for (int j = 0; j < array.length(); j++) {
                consumer.accept(key, array.getJSONObject(j));
            }
        } else if (node instanceof JSONObject obj) {
            consumer.accept(key, obj);
        } else {
            logger.warn("Attribute {} has unexpected value type: {}", key, node.getClass());
        }
    }

    // When this processor is used in a flow with a `Join Enrichment` processor, it harmonizes JSON among all processed entities,
    // for instance adding attributes which are not present by default in an entity.
    // In this case, these attributes are null or can have a null value.
    // Moreover, when doing a temporal request, if some attributes have no temporal values, they are still added, and they are null
    // So we filter out attributes that contain a null value or whose whole value is null
    private static void addAttributeIfValid(List<Attribute> attributes, Attribute attribute) {
        if (attribute != null &&
            attribute.getAttrValue() != null &&
            !Objects.equals(attribute.getAttrValue().toString(), "null"))
            attributes.add(attribute);
    }

    private static String normalizeAttributeName(String attributeName) {
        // Try to prevent from some JSON-LD contexts problems where an attribute ends up on the default vocab
        return attributeName.replace(DEFAULT_CORE_CONTEXT_PREFIX, "").toLowerCase();
    }
}
