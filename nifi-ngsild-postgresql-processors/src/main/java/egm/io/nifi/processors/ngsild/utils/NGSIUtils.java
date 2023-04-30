package egm.io.nifi.processors.ngsild.utils;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.stream.io.StreamUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class NGSIUtils {

    private static final Logger logger = LoggerFactory.getLogger(NGSIUtils.class);

    public static List<String> IGNORED_KEYS_ON_ATTRIBUTES =
            List.of("type", "value", "object", "datasetId", "createdAt", "modifiedAt", "instanceId", "observedAt");
    public static List<String> IGNORED_KEYS_ON_ENTITES = List.of("id", "type", "@context", "createdAt", "modifiedAt");

    public NGSIEvent getEventFromFlowFile(FlowFile flowFile, final ProcessSession session) {

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);

        Map<String, String> flowFileAttributes = flowFile.getAttributes();
        String ngsiLdTenant = flowFileAttributes.get("NGSILD-Tenant") == null ? "" : flowFileAttributes.get("NGSILD-Tenant");
        long creationTime = flowFile.getEntryDate();

        JSONArray content = new JSONArray(flowFileContent);
        List<Entity> entities = parseNgsiLdEntities(content);

        return new NGSIEvent(creationTime, ngsiLdTenant, entities);
    }

    public List<Entity> parseNgsiLdEntities(JSONArray content) {
        List<Entity> entities = new ArrayList<>();
        String entityType;
        String entityId;
        for (int i = 0; i < content.length(); i++) {
            JSONObject temporalEntity = content.getJSONObject(i);
            entityId = temporalEntity.getString("id");
            entityType = temporalEntity.getString("type");
            logger.debug("Dealing with entity {} of type {}", entityId, entityType);
            ArrayList<Attribute> attributes = new ArrayList<>();
            Iterator<String> keys = temporalEntity.keys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (!IGNORED_KEYS_ON_ENTITES.contains(key)) {
                    Object object = temporalEntity.get(key);
                    if (object instanceof JSONArray) {
                        // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                        JSONArray values = temporalEntity.getJSONArray(key);
                        for (int j = 0; j < values.length(); j++) {
                            JSONObject value = values.getJSONObject(j);
                            Attribute attribute = parseNgsiLdAttribute(key, value);
                            addAttributeIfValid(attributes, attribute);
                        }
                    } else if (object instanceof JSONObject) {
                        Attribute attribute = parseNgsiLdAttribute(key, (JSONObject) object);
                        addAttributeIfValid(attributes, attribute);
                    } else {
                        logger.warn("Attribute {} has unexpected value type: {}", key, object.getClass());
                    }
                }
            }

            entities.add(new Entity(entityId, entityType, attributes));
        }
        return entities;
    }

    private Attribute parseNgsiLdAttribute(String key, JSONObject value) {
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

        if ("Relationship".contentEquals(attrType)) {
            attrValue = value.get("object").toString();
        } else if ("Property".contentEquals(attrType)) {
            attrValue = value.opt("value");
        } else if ("GeoProperty".contentEquals(attrType)) {
            attrValue = value;
        } else if ("".contentEquals(attrType)){
            attrType = null;
            attrValue = null;
        } else {
            logger.warn("Unrecognized attribute type: {}", attrType);
            return null;
        }

        Iterator<String> keysOneLevel = value.keys();
        while (keysOneLevel.hasNext()) {
            String keyOne = keysOneLevel.next();
            if (("Property".equals(attrType) && "unitCode".equals(keyOne))) {
                if (value.get(keyOne) instanceof String)
                    subAttributes.add(new Attribute(keyOne.toLowerCase(), "Property", "", "", "", "", value.getString(keyOne), false, null));
            } else if ("RelationshipDetails".contains(keyOne)) {
                JSONObject relation = value.getJSONObject(keyOne);
                relation.remove("id");
                relation.remove("type");

                for (String relationKey : relation.keySet()) {
                    Object object = relation.get(relationKey);
                    if (object instanceof JSONArray) {
                        // it is a multi-attribute (see section 4.5.5 in NGSI-LD specification)
                        JSONArray valuesArray = relation.getJSONArray(relationKey);
                        for (int j = 0; j < valuesArray.length(); j++) {
                            JSONObject valueObject = valuesArray.getJSONObject(j);
                            Attribute subAttribute = parseNgsiLdSubAttribute(relationKey, valueObject);
                            addAttributeIfValid(subAttributes, subAttribute);
                        }
                    } else if (object instanceof JSONObject) {
                        Attribute subAttribute = parseNgsiLdSubAttribute(relationKey, (JSONObject) object);
                        addAttributeIfValid(subAttributes, subAttribute);
                    } else {
                        logger.warn("Sub Attribute {} has unexpected value type: {}", relationKey, object.getClass());
                    }
                }
            } else if (!IGNORED_KEYS_ON_ATTRIBUTES.contains(keyOne)) {
                Object object = value.get(keyOne);
                if (object instanceof JSONArray) {
                    JSONArray valuesArray = value.getJSONArray(keyOne);
                    for (int j = 0; j < valuesArray.length(); j++) {
                        JSONObject valueObject = valuesArray.getJSONObject(j);
                        Attribute subAttribute = parseNgsiLdSubAttribute(keyOne, valueObject);
                        addAttributeIfValid(subAttributes, subAttribute);
                    }
                } else if (object instanceof JSONObject) {
                    Attribute subAttribute = parseNgsiLdSubAttribute(keyOne, value.getJSONObject(keyOne));
                    addAttributeIfValid(subAttributes, subAttribute);
                } else {
                    logger.warn("Sub Attribute {} has unexpected value type: {}", keyOne, object.getClass());
                }
            }
        }

        return new Attribute(key.toLowerCase(), attrType, datasetId, observedAt, createdAt, modifiedAt, attrValue, !subAttributes.isEmpty(), subAttributes);
    }

    private Attribute parseNgsiLdSubAttribute(String key, JSONObject value) {
        String subAttrType = value.get("type").toString();
        Object subAttrValue = "";
        if ("Relationship".contentEquals(subAttrType)) {
            subAttrValue = value.get("object").toString();
        } else if ("Property".contentEquals(subAttrType)) {
            subAttrValue = value.get("value");
        } else if ("GeoProperty".contentEquals(subAttrType)) {
            subAttrValue = value.get("value").toString();
        }

        return new Attribute(key.toLowerCase(), subAttrType, "", "", "", "", subAttrValue, false, null);
    }

    // When this processor is used in a flow with a `Join Enrichment` processor, it harmonizes JSON among all processed entities,
    // for instance adding attributes which are not present by default in an entity.
    // In this case, these attributes are null or can have a null value.
    // Moreover, when doing a temporal request, if some attributes have no temporal values, they are still added, and they are null
    // So we filter out attributes that contain a null value or whose whole value is null
    private void addAttributeIfValid(List<Attribute> attributes, Attribute attribute) {
        if (attribute != null &&
                attribute.getAttrValue() != null &&
                !Objects.equals(attribute.getAttrValue().toString(), "null"))
            attributes.add(attribute);
    }
}
