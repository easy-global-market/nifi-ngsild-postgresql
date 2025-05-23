package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.NgsiLdConstants;
import egm.io.nifi.processors.ngsild.model.PostgreSQLConstants;
import egm.io.nifi.processors.ngsild.utils.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.processor.exception.ProcessException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.GENERIC_MEASURE;
import static egm.io.nifi.processors.ngsild.model.PostgreSQLConstants.POSTGRESQL_MAX_NAME_LEN;

public class PostgreSQLTransformer {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLTransformer.class);

    public Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields(
        Entity entity,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs,
        Set<String> ignoredAttributes
    ) {
        Map<String, POSTGRESQL_COLUMN_TYPES> aggregation = new TreeMap<>();

        Map<String, List<Attribute>> attributesByObservedAt =
            entity.getEntityAttrs().stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));

        aggregation.put(PostgreSQLConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
        aggregation.put(PostgreSQLConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
        aggregation.put(PostgreSQLConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
        if (entity.getScopes() != null) {
            aggregation.put(PostgreSQLConstants.ENTITY_SCOPES, POSTGRESQL_COLUMN_TYPES.ARRAY);
        }

        List<Attribute> attributes = new ArrayList<>();
        attributesByObservedAt.forEach((timestamp, attributesLd) -> attributesLd.forEach(attribute -> {
            if (!ignoredAttributes.contains(attribute.getAttrName()))
                attributes.add(attribute);
        }));

        for (Attribute attribute : attributes) {
            String attrName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), datasetIdPrefixToTruncate);
            if (isValidDate(attribute.getAttrValue().toString()))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.DATE);
            else if (isValidTime(attribute.getAttrValue().toString()))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TIMETZ);
            else if (isValidDateTime(attribute.getAttrValue().toString()))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            else if (attribute.getAttrValue() instanceof Number) {
                if (aggregation.replace(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC) == null)
                    aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
            } else if ("GeoProperty".equals(attribute.getAttrType())) {
                JSONObject geometryObject = (JSONObject) attribute.getAttrValue();
                if (geometryObject.getJSONObject("value").getString("type").equals("Point")) {
                    String encodedGeopropertyLon = encodeAttributeToColumnName(attribute.getAttrName(), "lon", datasetIdPrefixToTruncate);
                    String encodedGeopropertyLat = encodeAttributeToColumnName(attribute.getAttrName(), "lat", datasetIdPrefixToTruncate);
                    aggregation.put(encodedGeopropertyLon, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                    aggregation.put(encodedGeopropertyLat, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                }
                String encodedGeometry = encodeAttributeToColumnName(attribute.getAttrName(), "geometry", datasetIdPrefixToTruncate);
                String encodedGeoJson = encodeAttributeToColumnName(attribute.getAttrName(), "geojson", datasetIdPrefixToTruncate);
                aggregation.put(encodedGeometry, POSTGRESQL_COLUMN_TYPES.GEOMETRY);
                aggregation.put(encodedGeoJson, POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
            } else if ("JsonProperty".equals(attribute.getAttrType())) {
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.JSONB);
            } else
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);

            logger.debug("Added {} in the list of fields for entity {}", attrName, entity.entityId);

            if (!attribute.observedAt.isEmpty()) {
                String encodedObservedAt = encodeTimePropertyToColumnName(attrName, NgsiLdConstants.OBSERVED_AT);
                aggregation.put(encodedObservedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            } else if (exportSysAttrs) {
                String encodedModifiedAt = encodeTimePropertyToColumnName(attrName, NgsiLdConstants.MODIFIED_AT);
                aggregation.put(encodedModifiedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);

                String encodedCreatedAt = encodeTimePropertyToColumnName(attrName, NgsiLdConstants.CREATED_AT);
                aggregation.put(encodedCreatedAt, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            }

            if (attribute.isHasSubAttrs()) {
                for (Attribute subAttribute : attribute.getSubAttrs()) {
                    if (!ignoredAttributes.contains(subAttribute.getAttrName())) {
                        String subAttrName = subAttribute.getAttrName();
                        String encodedSubAttrName =
                            encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttrName, datasetIdPrefixToTruncate);
                        if (subAttribute.getAttrValue() instanceof Number)
                            aggregation.put(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                        else
                            aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                        logger.debug("Added subattribute {} ({}) to attribute {}", encodedSubAttrName, subAttrName, attrName);
                    }
                }
            }
        }

        return aggregation;
    }

    private String encodeAttributeToColumnName(String attributeName, String datasetId, String datasetIdPrefixToTruncate) {
        // For too long dataset ids, truncate to 32 (not perfect, nor totally bulletproof)
        String datasetIdEncodedValue =
            (!datasetId.isEmpty() ?
                "_" + PostgreSQLUtils.encodePostgreSQL(PostgreSQLUtils.truncateToSize(datasetId.replaceFirst(datasetIdPrefixToTruncate, ""), 32)) :
                ""
            );
        String encodedName = PostgreSQLUtils.encodePostgreSQL(attributeName) + datasetIdEncodedValue;
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName).toLowerCase();
    }

    private String encodeTimePropertyToColumnName(String encodedAttributeName, String timeProperty) {
        String encodedName = encodedAttributeName + "_" + PostgreSQLUtils.encodePostgreSQL(timeProperty);
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName).toLowerCase();
    }

    private String encodeSubAttributeToColumnName(String attributeName, String datasetId, String subAttributeName, String datasetIdPrefixToTruncate) {
        String encodedAttributeName = encodeAttributeToColumnName(attributeName, datasetId, datasetIdPrefixToTruncate);
        String encodedName = encodedAttributeName + "_" + PostgreSQLUtils.encodePostgreSQL(subAttributeName);
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName).toLowerCase();
    }

    public List<String> getValuesForInsert(
        Entity entity,
        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
        long creationTime,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs,
        Boolean ignoreEmptyObservedAt,
        Boolean flattenObservations
    ) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        List<String> valuesForInsertList = new ArrayList<>();
        Map<String, List<Attribute>> attributesByObservedAt =
            entity.getEntityAttrs().stream()
                .collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        List<String> observedTimestamps =
            attributesByObservedAt.keySet().stream()
                .sorted()
                .collect(Collectors.toList());
        // get all the attributes without an observedAt timestamp to inject them as is in each row
        List<Attribute> attributesWithoutObservedAt =
            entity.getEntityAttrs().stream()
                .filter(attribute -> attribute.observedAt == null || attribute.observedAt.isEmpty())
                .collect(Collectors.toList());

        String oldestTimeStamp;
        if (observedTimestamps.get(0).isEmpty()) {
            if (observedTimestamps.size() > 1)
                oldestTimeStamp = observedTimestamps.get(1);
            else
                oldestTimeStamp = DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC));
        } else
            oldestTimeStamp = observedTimestamps.get(0);

        for (String observedTimestamp : observedTimestamps) {
            Map<String, String> valuesForColumns = new TreeMap<>();
            if (!flattenObservations) {
                for (Attribute attribute : attributesByObservedAt.get(observedTimestamp)) {
                    Map<String, String> attributesValues =
                        insertAttributesValues(attribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                            creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                    valuesForColumns.putAll(attributesValues);
                }
                for (Attribute attribute : attributesWithoutObservedAt) {
                    Map<String, String> attributesValues =
                        insertAttributesValues(attribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                            creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                    valuesForColumns.putAll(attributesValues);
                }
                List<String> listofEncodedName = new ArrayList<>(listOfFields.keySet());
                for (String s : listofEncodedName) {
                    valuesForColumns.putIfAbsent(s, null);
                }
                boolean hasObservations = valuesForColumns.entrySet().stream().anyMatch(entry ->
                    entry.getKey().endsWith("observedat") && entry.getValue() != null);
                if (hasObservations || !ignoreEmptyObservedAt)
                    valuesForInsertList.add("(" + String.join(",", valuesForColumns.values()) + ")");
            } else {
                // when flattening observations, there may have more than one row per observation date
                List<Attribute> attributes = attributesByObservedAt.get(observedTimestamp);
                List<Attribute> commonAttributes =
                    attributes.stream()
                        .filter(attribute -> !Objects.equals(attribute.getAttrName(), GENERIC_MEASURE))
                        .collect(Collectors.toList());
                // first fill with the common attributes (the non observed ones)
                for (Attribute commonAttribute : commonAttributes) {
                    Map<String, String> attributesValues =
                        insertAttributesValues(commonAttribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                            creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                    valuesForColumns.putAll(attributesValues);
                }
                List<Attribute> observedAttributes =
                    attributes.stream()
                        .filter(attribute -> Objects.equals(attribute.getAttrName(), GENERIC_MEASURE))
                        .collect(Collectors.toList());
                // then for each observed attribute, add a new row
                for (Attribute observedAttribute : observedAttributes) {
                    Map<String, String> attributesValues =
                        insertAttributesValues(observedAttribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                            creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                    valuesForColumns.putAll(attributesValues);

                    List<String> listofEncodedName = new ArrayList<>(listOfFields.keySet());
                    for (String s : listofEncodedName) {
                        valuesForColumns.putIfAbsent(s, null);
                    }
                    valuesForInsertList.add("(" + String.join(",", valuesForColumns.values()) + ")");
                }
            }
        }

        return valuesForInsertList;
    }

    private Map<String, String> insertAttributesValues(
        Attribute attribute,
        Map<String, String> valuesForColumns,
        Entity entity,
        String oldestTimeStamp,
        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
        long creationTime,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs
    ) {
        String encodedAttributeName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), datasetIdPrefixToTruncate);
        // some attributes may have been set to be ignored, don't add values for them
        if (!listOfFields.containsKey(encodedAttributeName))
            return valuesForColumns;

        ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);

        valuesForColumns.put(PostgreSQLConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(creationDate) + "'");
        valuesForColumns.put(PostgreSQLConstants.ENTITY_ID, "'" + entity.getEntityId() + "'");
        valuesForColumns.put(PostgreSQLConstants.ENTITY_TYPE, "'" + entity.getEntityType() + "'");
        if (entity.getScopes() != null)
            valuesForColumns.put(PostgreSQLConstants.ENTITY_SCOPES, "'{" + String.join(",", entity.getScopes()) + "}'");

        if ("GeoProperty".equals(attribute.getAttrType())) {
            JSONObject geoProppertyObject = (JSONObject) attribute.getAttrValue();
            JSONObject geoJsonObject = geoProppertyObject.getJSONObject("value");
            JSONArray location = (JSONArray) geoJsonObject.get("coordinates");
            if (geoJsonObject.getString("type").equals("Point")) {
                String encodedGeopropertyLon = encodeAttributeToColumnName(attribute.getAttrName(), "lon", datasetIdPrefixToTruncate);
                String encodedGeopropertyLat = encodeAttributeToColumnName(attribute.getAttrName(), "lat", datasetIdPrefixToTruncate);

                valuesForColumns.put(encodedGeopropertyLon, formatFieldForValueInsert(location.getDouble(0), listOfFields.get(encodedGeopropertyLon)));
                valuesForColumns.put(encodedGeopropertyLat, formatFieldForValueInsert(location.getDouble(1), listOfFields.get(encodedGeopropertyLat)));
            }
            JSONObject geoJson = new JSONObject();
            geoJson.put("type", "Feature");
            // to be correctly rendered, viz tools often require a properties object into the GeoJSON object
            // so add one containing the entity id (only thing common to all entities)
            JSONObject geoJsonProperties = new JSONObject();
            geoJsonProperties.put(PostgreSQLConstants.ENTITY_ID, entity.entityId);
            geoJson.put("properties", geoJsonProperties);
            geoJson.put("geometry", geoJsonObject);

            String encodedGeometry = encodeAttributeToColumnName(attribute.getAttrName(), "geometry", datasetIdPrefixToTruncate);
            String encodedGeoJson = encodeAttributeToColumnName(attribute.getAttrName(), "geojson", datasetIdPrefixToTruncate);

            valuesForColumns.put(encodedGeometry, formatFieldForValueInsert(geoJsonObject, listOfFields.get(encodedGeometry)));
            valuesForColumns.put(encodedGeoJson, formatFieldForValueInsert(geoJson, listOfFields.get(encodedGeoJson)));
            valuesForColumns.put(encodedAttributeName, formatFieldForValueInsert(location, listOfFields.get(encodedAttributeName)));
        } else {
            valuesForColumns.put(encodedAttributeName, formatFieldForValueInsert(attribute.getAttrValue(), listOfFields.get(encodedAttributeName)));
        }

        if (!attribute.getObservedAt().isEmpty()) {
            String encodedObservedAt = encodeTimePropertyToColumnName(encodedAttributeName, NgsiLdConstants.OBSERVED_AT);
            valuesForColumns.put(encodedObservedAt, formatFieldForValueInsert(attribute.getObservedAt(), listOfFields.get(encodedObservedAt)));
        } else if (exportSysAttrs) {
            String encodedCreatedAt = encodeTimePropertyToColumnName(encodedAttributeName, NgsiLdConstants.CREATED_AT);
            if (attribute.createdAt == null ||
                attribute.createdAt.isEmpty() ||
                ZonedDateTime.parse(attribute.createdAt).toEpochSecond() > ZonedDateTime.parse(oldestTimeStamp).toEpochSecond()
            ) {
                valuesForColumns.put(encodedCreatedAt, formatFieldForValueInsert(oldestTimeStamp, listOfFields.get(encodedCreatedAt)));
            } else
                valuesForColumns.put(encodedCreatedAt, formatFieldForValueInsert(attribute.createdAt, listOfFields.get(encodedCreatedAt)));

            String encodedModifiedAt = encodeTimePropertyToColumnName(encodedAttributeName, NgsiLdConstants.MODIFIED_AT);
            if (attribute.modifiedAt != null && !attribute.modifiedAt.isEmpty()) {
                valuesForColumns.put(encodedModifiedAt, formatFieldForValueInsert(attribute.modifiedAt, listOfFields.get(encodedModifiedAt)));
            }
        }

        if (attribute.isHasSubAttrs()) {
            for (Attribute subAttribute : attribute.getSubAttrs()) {
                String encodedSubAttributeName =
                    encodeSubAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName(), datasetIdPrefixToTruncate);
                if (listOfFields.containsKey(encodedSubAttributeName))
                    valuesForColumns.put(encodedSubAttributeName, formatFieldForValueInsert(subAttribute.getAttrValue(), listOfFields.get(encodedSubAttributeName)));
            }
        }

        return valuesForColumns;
    }

    private String formatFieldForValueInsert(Object attributeValue, POSTGRESQL_COLUMN_TYPES columnType) {
        String formattedField;
        switch (columnType) {
            case NUMERIC:
                if (attributeValue instanceof Number) formattedField = attributeValue.toString();
                else formattedField = null;
                break;
            case TIMESTAMPTZ:
            case DATE:
            case TIMETZ:
            case JSONB:
                formattedField = "'" + attributeValue + "'";
                break;
            case GEOMETRY:
                formattedField = "ST_GeomFromGeoJSON('" + attributeValue + "')";
                break;
            default:
                formattedField = "$$" + attributeValue + "$$";
        }
        return formattedField;
    }

    public String getFieldsForCreate(Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        Iterator<Map.Entry<String, POSTGRESQL_COLUMN_TYPES>> it = listOfFields.entrySet().iterator();
        String fieldsForCreate = "(";
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<String, POSTGRESQL_COLUMN_TYPES> entry = it.next();
            if (first) {
                fieldsForCreate += entry.getKey() + " " + entry.getValue().getPostgresType();
                first = false;
            } else {
                fieldsForCreate += "," + entry.getKey() + " " + entry.getValue().getPostgresType();
            } // if else
        } // while

        return fieldsForCreate + ")";
    }

    public String getFieldsForInsert(Set<String> listOfFieldsNames) {
        return "(" + String.join(",", listOfFieldsNames) + ")";
    }

    public String buildSchemaName(String tenantName) throws Exception {
        String schemaName = PostgreSQLUtils.encodePostgreSQL(tenantName);
        if (schemaName.length() > POSTGRESQL_MAX_NAME_LEN) {
            String errorMessage = "Building schema name '" + schemaName + "' and its length is greater than " + POSTGRESQL_MAX_NAME_LEN;
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }
        return schemaName;
    }

    public String createSchema(String schemaName) {
        return "create schema if not exists " + schemaName + ";";
    }

    public String createTable(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        return "create table if not exists " + schemaName + "." + tableName + " " + getFieldsForCreate(listOfFields) + ";";
    }

    public String buildTableName(Entity entity, String tableNameSuffix) throws Exception {
        String tableName;
        String entityType = entity.getEntityType();

        if (tableNameSuffix != null && !tableNameSuffix.isEmpty())
            tableName = PostgreSQLUtils.encodePostgreSQL(entityType) +
                PostgreSQLConstants.OLD_CONCATENATOR +
                PostgreSQLUtils.encodePostgreSQL(tableNameSuffix);
        else tableName = PostgreSQLUtils.encodePostgreSQL(entityType);

        if (tableName.length() > POSTGRESQL_MAX_NAME_LEN) {
            String errorMessage = "Building table name '" + tableName + "' and its length is greater than " + POSTGRESQL_MAX_NAME_LEN;
            logger.error(errorMessage);
            throw new Exception(errorMessage);
        }
        return tableName;
    }

    public String insertQuery(
        Entity entity,
        long creationTime,
        String schemaName,
        String tableName,
        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs,
        Boolean ignoreEmptyObservedAt,
        Boolean flattenObservations
    ) {
        List<String> valuesForInsert =
            this.getValuesForInsert(entity, listOfFields, creationTime, datasetIdPrefixToTruncate, exportSysAttrs, ignoreEmptyObservedAt, flattenObservations);

        if (valuesForInsert.isEmpty()) {
            logger.warn("Unable to get values to insert for {}, returning fake statement", entity.entityId);
            return "select 1;";
        } else
            return "insert into " + schemaName + "." + tableName + " " + this.getFieldsForInsert(listOfFields.keySet()) + " values " + String.join(",", valuesForInsert) + ";";
    }

    public String checkColumnNames(String tableName) {
        return "select column_name from information_schema.columns where table_name ='" + tableName + "';";
    }

    public String getColumnsTypes(String tableName) {
        return "select column_name, udt_name from information_schema.columns where table_name ='" + tableName + "';";
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getUpdatedListOfTypedFields(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) throws SQLException {
        // create an initial map containing all the fields with columns names in lowercase
        // Get the column names; column indices start from 1
        while (rs.next()) {
            POSTGRESQL_COLUMN_TYPES postgresqlColumnTypes;
            if (rs.getString(2).equals("_text"))
                postgresqlColumnTypes = POSTGRESQL_COLUMN_TYPES.ARRAY;
            else
                postgresqlColumnTypes = POSTGRESQL_COLUMN_TYPES.valueOf(rs.getString(2).toUpperCase());
            Pair<String, POSTGRESQL_COLUMN_TYPES> columnNameWithDataType =
                new ImmutablePair<>(rs.getString(1), postgresqlColumnTypes);
            if (listOfFields.containsKey(columnNameWithDataType.getKey()) &&
                listOfFields.get(columnNameWithDataType.getKey()) != columnNameWithDataType.getValue()) {
                logger.info("Column {} with type {} already existed with a different type {}",
                    columnNameWithDataType.getKey(),
                    listOfFields.get(columnNameWithDataType.getKey()),
                    columnNameWithDataType.getValue()
                );
                // update the column type to avoid type inconsistencies when inserting new values
                // if a value in an entity does not match the current type in DB, a NULL value will be used
                listOfFields.replace(columnNameWithDataType.getKey(), columnNameWithDataType.getValue());
            }
        }

        return listOfFields;
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getNewColumns(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) throws SQLException {
        // create an initial map containing all the fields with columns names in lowercase
        Map<String, POSTGRESQL_COLUMN_TYPES> newFields = new HashMap<>(listOfFields).entrySet().stream().map(e -> Map.entry(e.getKey().toLowerCase(), e.getValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Get the column names; column indices start from 1
        while (rs.next()) {
            String columnName = rs.getString(1);
            logger.debug("Looking at column {} (exists: {})", columnName, newFields.containsKey(columnName));
            newFields.remove(columnName);
        }

        logger.debug("New columns to create: {}", newFields.keySet());

        return newFields;
    }

    public String addColumns(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> columnNames) {
        Iterator<Map.Entry<String, POSTGRESQL_COLUMN_TYPES>> it = columnNames.entrySet().iterator();
        String fieldsForCreate = "";
        boolean first = true;
        while (it.hasNext()) {
            Map.Entry<String, POSTGRESQL_COLUMN_TYPES> entry = it.next();
            if (first) {
                fieldsForCreate += " ADD COLUMN " + entry.getKey() + " " + entry.getValue().getPostgresType();
                first = false;
            } else {
                fieldsForCreate += ", ADD COLUMN " + entry.getKey() + " " + entry.getValue().getPostgresType();
            } // if else
        } // while

        fieldsForCreate += ";";

        return "alter table " + schemaName + "." + tableName + fieldsForCreate;
    }

    private boolean isValidDate(String date) {
        try {
            DateTimeFormatter.ISO_DATE.parse(date);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    private boolean isValidTime(String time) {
        try {
            DateTimeFormatter.ISO_TIME.parse(time);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    private boolean isValidDateTime(String dateTime) {
        try {
            DateTimeFormatter.ISO_DATE_TIME.parse(dateTime);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

    public enum POSTGRESQL_COLUMN_TYPES {
        TEXT,
        TIMESTAMPTZ,
        TIMETZ,
        DATE,
        NUMERIC,
        GEOMETRY,
        JSONB,
        ARRAY;

        public String getPostgresType() {
            if (this == POSTGRESQL_COLUMN_TYPES.ARRAY) {
                return "text[]";
            }
            return this.name();
        }
    }
}
