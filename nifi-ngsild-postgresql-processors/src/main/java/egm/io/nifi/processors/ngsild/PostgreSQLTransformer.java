package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.model.Attribute;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.ExportMode;
import egm.io.nifi.processors.ngsild.model.NgsiLdConstants;
import egm.io.nifi.processors.ngsild.model.PostgreSQLConstants;
import egm.io.nifi.processors.ngsild.utils.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.GENERIC_MEASURE;
import static egm.io.nifi.processors.ngsild.model.NgsiLdConstants.OBSERVED_AT;
import static egm.io.nifi.processors.ngsild.model.PostgreSQLConstants.POSTGRESQL_MAX_NAME_LEN;

public class PostgreSQLTransformer {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLTransformer.class);

    private static final Pattern UUID_REGEX =
        Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    public Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields(
        Entity entity,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs,
        Set<String> ignoredAttributes
    ) {
        Map<String, POSTGRESQL_COLUMN_TYPES> aggregation = new TreeMap<>();

        aggregation.put(PostgreSQLConstants.RECV_TIME, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
        aggregation.put(PostgreSQLConstants.ENTITY_ID, POSTGRESQL_COLUMN_TYPES.TEXT);
        aggregation.put(PostgreSQLConstants.ENTITY_TYPE, POSTGRESQL_COLUMN_TYPES.TEXT);
        if (entity.getScopes() != null) {
            aggregation.put(PostgreSQLConstants.ENTITY_SCOPES, POSTGRESQL_COLUMN_TYPES.ARRAY);
        }

        List<Attribute> attributes = entity.getEntityAttrs().stream()
            .filter(attr -> !ignoredAttributes.contains(attr.getAttrName()))
            .collect(Collectors.toList());

        for (Attribute attribute : attributes) {
            String attrName = encodeAttributeToColumnName(attribute.getAttrName(), attribute.getDatasetId(), datasetIdPrefixToTruncate);
            String attrValueStr = attribute.getAttrValue().toString();

            if (parsesWith(DateTimeFormatter.ISO_DATE, attrValueStr))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.DATE);
            else if (parsesWith(DateTimeFormatter.ISO_TIME, attrValueStr))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TIMETZ);
            else if (parsesWith(DateTimeFormatter.ISO_DATE_TIME, attrValueStr))
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            else if (attribute.getAttrValue() instanceof Number)
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
            else if (NgsiLdConstants.ATTR_TYPE_GEO_PROPERTY.equals(attribute.getAttrType())) {
                JSONObject geometryObject = (JSONObject) attribute.getAttrValue();
                if (geometryObject.getJSONObject("value").getString("type").equals("Point")) {
                    aggregation.put(encodeAttributeToColumnName(attribute.getAttrName(), "lon", datasetIdPrefixToTruncate), POSTGRESQL_COLUMN_TYPES.NUMERIC);
                    aggregation.put(encodeAttributeToColumnName(attribute.getAttrName(), "lat", datasetIdPrefixToTruncate), POSTGRESQL_COLUMN_TYPES.NUMERIC);
                }
                aggregation.put(encodeAttributeToColumnName(attribute.getAttrName(), "geometry", datasetIdPrefixToTruncate), POSTGRESQL_COLUMN_TYPES.GEOMETRY);
                aggregation.put(encodeAttributeToColumnName(attribute.getAttrName(), "geojson", datasetIdPrefixToTruncate), POSTGRESQL_COLUMN_TYPES.TEXT);
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
            } else if (NgsiLdConstants.ATTR_TYPE_JSON_PROPERTY.equals(attribute.getAttrType())) {
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.JSONB);
            } else {
                aggregation.put(attrName, POSTGRESQL_COLUMN_TYPES.TEXT);
            }

            logger.debug("Added {} in the list of fields for entity {}", attrName, entity.entityId);

            if (!attribute.observedAt.isEmpty()) {
                aggregation.put(encodeTimePropertyToColumnName(attrName, NgsiLdConstants.OBSERVED_AT), POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            } else if (exportSysAttrs) {
                aggregation.put(encodeTimePropertyToColumnName(attrName, NgsiLdConstants.MODIFIED_AT), POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
                aggregation.put(encodeTimePropertyToColumnName(attrName, NgsiLdConstants.CREATED_AT), POSTGRESQL_COLUMN_TYPES.TIMESTAMPTZ);
            }

            if (attribute.hasSubAttrs()) {
                for (Attribute subAttribute : attribute.getSubAttrs()) {
                    if (!ignoredAttributes.contains(subAttribute.getAttrName())) {
                        String encodedSubAttrName = encodeSubAttributeToColumnName(
                            attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName(), datasetIdPrefixToTruncate
                        );
                        if (subAttribute.getAttrValue() instanceof Number)
                            aggregation.put(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.NUMERIC);
                        else
                            aggregation.putIfAbsent(encodedSubAttrName, POSTGRESQL_COLUMN_TYPES.TEXT);
                        logger.debug("Added subattribute {} ({}) to attribute {}", encodedSubAttrName, subAttribute.getAttrName(), attrName);
                    }
                }
            }
        }

        return aggregation;
    }

    private String encodeAttributeToColumnName(String attributeName, String datasetId, String datasetIdPrefixToTruncate) {
        String datasetIdWithoutPrefix = datasetId.replaceFirst(datasetIdPrefixToTruncate, "");
        if (UUID_REGEX.matcher(datasetIdWithoutPrefix).matches())
            datasetIdWithoutPrefix = datasetIdWithoutPrefix.substring(0, 8);

        // For too long dataset ids, truncate to 32 (not perfect, nor totally bulletproof)
        String datasetIdEncodedValue =
            (!datasetId.isEmpty() ?
                "_" + PostgreSQLUtils.encodePostgreSQL(PostgreSQLUtils.truncateToSize(datasetIdWithoutPrefix, 32)) :
                ""
            );
        String encodedName = PostgreSQLUtils.encodePostgreSQL(attributeName) + datasetIdEncodedValue;
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName);
    }

    private String encodeTimePropertyToColumnName(String encodedAttributeName, String timeProperty) {
        String encodedName = Objects.equals(timeProperty, OBSERVED_AT) && !Objects.equals(encodedAttributeName, GENERIC_MEASURE)
            ? PostgreSQLUtils.encodePostgreSQL(timeProperty)
            : encodedAttributeName + "_" + PostgreSQLUtils.encodePostgreSQL(timeProperty);
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName);
    }

    private String encodeSubAttributeToColumnName(String attributeName, String datasetId, String subAttributeName, String datasetIdPrefixToTruncate) {
        String encodedAttributeName = encodeAttributeToColumnName(attributeName, datasetId, datasetIdPrefixToTruncate);
        String encodedName = encodedAttributeName + "_" + PostgreSQLUtils.encodePostgreSQL(subAttributeName);
        return PostgreSQLUtils.truncateToMaxPgSize(encodedName);
    }

    public List<String> getValuesForInsert(
        Entity entity,
        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
        long creationTime,
        String datasetIdPrefixToTruncate,
        Boolean exportSysAttrs,
        Boolean ignoreEmptyObservedAt,
        ExportMode exportMode
    ) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        List<String> valuesForInsertList = new ArrayList<>();
        Map<String, List<Attribute>> attributesByObservedAt = groupByObservedAt(entity);
        List<String> observedTimestamps = attributesByObservedAt.keySet().stream().sorted().toList();
        // get all the attributes without an observedAt timestamp to inject them as is in each row
        List<Attribute> attributesWithoutObservedAt = entity.getEntityAttrs().stream()
            .filter(attribute -> attribute.observedAt == null || attribute.observedAt.isEmpty())
            .toList();

        if (observedTimestamps.isEmpty()) {
            return valuesForInsertList;
        }
        String oldestTimeStamp = resolveOldestTimestamp(observedTimestamps, creationTime);

        for (String observedTimestamp : observedTimestamps) {
            Map<String, String> valuesForColumns = new TreeMap<>();

            // 1. Handle non-temporal rows (Current State)
            if (observedTimestamp.isEmpty()) {
                // Non-temporal row is only created for Expanded/Semi-Flatten and if explicitly allowed
                if (ignoreEmptyObservedAt || ExportMode.FLATTEN.equals(exportMode)) {
                    continue;
                }
                for (Attribute attribute : attributesWithoutObservedAt) {
                    insertAttributesValues(attribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                        creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                }
                finalizeAndAddRow(valuesForColumns, listOfFields, valuesForInsertList, false);
                continue;
            }

            // 2. Handle temporal rows (Observations)
            // Pre-fill with attributes that don't have an observedAt (metadata, etc.)
            for (Attribute commonAttribute : attributesWithoutObservedAt) {
                insertAttributesValues(commonAttribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                    creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
            }

            List<Attribute> observedAttributes = attributesByObservedAt.get(observedTimestamp);

            if (ExportMode.EXPANDED.equals(exportMode)) {
                // Expanded: Merge all attributes of the same timestamp into one row
                for (Attribute attribute : observedAttributes) {
                    insertAttributesValues(attribute, valuesForColumns, entity, oldestTimeStamp, listOfFields,
                        creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                }
                finalizeAndAddRow(valuesForColumns, listOfFields, valuesForInsertList, true);
            } else {
                // Flatten and Semi-Flatten: One row per attribute instance
                for (Attribute observedAttribute : observedAttributes) {
                    if (observedAttribute.hasSubAttrs() || Objects.equals(observedAttribute.getAttrName(), GENERIC_MEASURE)) {
                        Map<String, String> rowValues = new TreeMap<>(valuesForColumns);
                        insertAttributesValues(observedAttribute, rowValues, entity, oldestTimeStamp, listOfFields,
                            creationTime, datasetIdPrefixToTruncate, exportSysAttrs);
                        finalizeAndAddRow(rowValues, listOfFields, valuesForInsertList, true);
                    }
                }
            }
        }

        return valuesForInsertList;
    }

    private Map<String, List<Attribute>> groupByObservedAt(Entity entity) {
        return entity.getEntityAttrs().stream().collect(Collectors.groupingBy(a -> a.observedAt));
    }

    private String resolveOldestTimestamp(List<String> observedTimestamps, long creationTime) {
        if (observedTimestamps.get(0).isEmpty()) {
            if (observedTimestamps.size() > 1)
                return observedTimestamps.get(1);
            else
                return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC));
        }
        return observedTimestamps.get(0);
    }

    private void finalizeAndAddRow(
        Map<String, String> rowValues,
        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields,
        List<String> valuesList,
        boolean ignoreIfNoObservedAt
    ) {
        for (String fieldName : listOfFields.keySet()) {
            rowValues.putIfAbsent(fieldName, null);
        }

        boolean hasObservations = rowValues.entrySet().stream().anyMatch(entry ->
            entry.getKey().endsWith("observedat") && entry.getValue() != null);

        if (hasObservations || !ignoreIfNoObservedAt) {
            valuesList.add("(" + String.join(",", rowValues.values()) + ")");
        }
    }

    private void insertAttributesValues(
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
            return;

        ZonedDateTime creationDate = Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC);

        valuesForColumns.put(PostgreSQLConstants.RECV_TIME, "'" + DateTimeFormatter.ISO_INSTANT.format(creationDate) + "'");
        valuesForColumns.put(PostgreSQLConstants.ENTITY_ID, "'" + entity.getEntityId() + "'");
        valuesForColumns.put(PostgreSQLConstants.ENTITY_TYPE, "'" + entity.getEntityType() + "'");
        if (entity.getScopes() != null)
            valuesForColumns.put(PostgreSQLConstants.ENTITY_SCOPES, "'{" + String.join(",", entity.getScopes()) + "}'");

        if (NgsiLdConstants.ATTR_TYPE_GEO_PROPERTY.equals(attribute.getAttrType())) {
            JSONObject geoProppertyObject = (JSONObject) attribute.getAttrValue();
            JSONObject geoJsonObject = geoProppertyObject.getJSONObject("value");
            JSONArray location = (JSONArray) geoJsonObject.get("coordinates");
            if (geoJsonObject.getString("type").equals("Point")) {
                valuesForColumns.put(
                    encodeAttributeToColumnName(attribute.getAttrName(), "lon", datasetIdPrefixToTruncate),
                    formatFieldForValueInsert(location.getDouble(0), listOfFields.get(encodeAttributeToColumnName(attribute.getAttrName(), "lon", datasetIdPrefixToTruncate)))
                );
                valuesForColumns.put(
                    encodeAttributeToColumnName(attribute.getAttrName(), "lat", datasetIdPrefixToTruncate),
                    formatFieldForValueInsert(location.getDouble(1), listOfFields.get(encodeAttributeToColumnName(attribute.getAttrName(), "lat", datasetIdPrefixToTruncate)))
                );
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
            } else {
                valuesForColumns.put(encodedCreatedAt, formatFieldForValueInsert(attribute.createdAt, listOfFields.get(encodedCreatedAt)));
            }

            String encodedModifiedAt = encodeTimePropertyToColumnName(encodedAttributeName, NgsiLdConstants.MODIFIED_AT);
            if (attribute.modifiedAt != null && !attribute.modifiedAt.isEmpty()) {
                valuesForColumns.put(encodedModifiedAt, formatFieldForValueInsert(attribute.modifiedAt, listOfFields.get(encodedModifiedAt)));
            }
        }

        if (attribute.hasSubAttrs()) {
            for (Attribute subAttribute : attribute.getSubAttrs()) {
                String encodedSubAttributeName = encodeSubAttributeToColumnName(
                    attribute.getAttrName(), attribute.getDatasetId(), subAttribute.getAttrName(), datasetIdPrefixToTruncate
                );
                if (listOfFields.containsKey(encodedSubAttributeName))
                    valuesForColumns.put(encodedSubAttributeName, formatFieldForValueInsert(subAttribute.getAttrValue(), listOfFields.get(encodedSubAttributeName)));
            }
        }
    }

    private String formatFieldForValueInsert(Object attributeValue, POSTGRESQL_COLUMN_TYPES columnType) {
        return switch (columnType) {
            case NUMERIC -> (attributeValue instanceof Number) ? attributeValue.toString() : null;
            case TIMESTAMPTZ, DATE, TIMETZ, JSONB -> "'" + attributeValue + "'";
            case GEOMETRY -> "ST_GeomFromGeoJSON('" + attributeValue + "')";
            default -> "$$" + attributeValue + "$$";
        };
    }

    public String getFieldsForCreate(Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        String columns = listOfFields.entrySet().stream()
            .map(e -> e.getKey() + " " + e.getValue().getPostgresType())
            .collect(Collectors.joining(","));
        return "(" + columns + ")";
    }

    public String getFieldsForInsert(Set<String> listOfFieldsNames) {
        return "(" + String.join(",", listOfFieldsNames) + ")";
    }

    public String buildSchemaName(String tenantName) {
        String schemaName = PostgreSQLUtils.encodePostgreSQL(tenantName);
        assertFitsNameLimit(schemaName, "schema name");
        return schemaName;
    }

    public String createSchema(String schemaName) {
        return "create schema if not exists " + schemaName + ";";
    }

    public String createTable(String schemaName, String tableName, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) {
        return "create table if not exists " + schemaName + "." + tableName + " " + getFieldsForCreate(listOfFields) + ";";
    }

    public String buildTableName(Entity entity, String tableNameSuffix) {
        String entityType = entity.getEntityType();
        String tableName = (tableNameSuffix != null && !tableNameSuffix.isEmpty())
            ? PostgreSQLUtils.encodePostgreSQL(entityType) + PostgreSQLConstants.NAME_SEPARATOR + PostgreSQLUtils.encodePostgreSQL(tableNameSuffix)
            : PostgreSQLUtils.encodePostgreSQL(entityType);
        assertFitsNameLimit(tableName, "table name");
        return tableName;
    }

    private void assertFitsNameLimit(String name, String label) {
        if (name.length() > POSTGRESQL_MAX_NAME_LEN) {
            String errorMessage = "Building " + label + " '" + name + "' and its length is greater than " + POSTGRESQL_MAX_NAME_LEN;
            logger.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
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
        ExportMode exportMode
    ) {
        List<String> valuesForInsert =
            this.getValuesForInsert(entity, listOfFields, creationTime, datasetIdPrefixToTruncate, exportSysAttrs, ignoreEmptyObservedAt, exportMode);

        if (valuesForInsert.isEmpty()) {
            logger.warn("Unable to get values to insert for {}, returning fake statement", entity.entityId);
            return "select 1;";
        } else {
            return "insert into " + schemaName + "." + tableName + " " + this.getFieldsForInsert(listOfFields.keySet()) + " values " + String.join(",", valuesForInsert) + ";";
        }
    }

    public String checkColumnNames(String tableName) {
        return "select column_name from information_schema.columns where table_name ='" + tableName + "';";
    }

    public String getColumnsTypes(String tableName) {
        return "select column_name, udt_name from information_schema.columns where table_name ='" + tableName + "';";
    }

    public String deleteEntityQuery(String schemaName, String tableName, String entityId) {
        return "delete from " + schemaName + "." + tableName + " where " + PostgreSQLConstants.ENTITY_ID + " = '" + entityId + "';";
    }

    public Map<String, POSTGRESQL_COLUMN_TYPES> getUpdatedListOfTypedFields(ResultSet rs, Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields) throws SQLException {
        // create an initial map containing all the fields with columns names in lowercase
        // Get the column names; column indices start from 1
        while (rs.next()) {
            POSTGRESQL_COLUMN_TYPES postgresqlColumnTypes = rs.getString(2).equals("_text")
                ? POSTGRESQL_COLUMN_TYPES.ARRAY
                : POSTGRESQL_COLUMN_TYPES.valueOf(rs.getString(2).toUpperCase());
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
        Map<String, POSTGRESQL_COLUMN_TYPES> newFields = listOfFields.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue, (a, b) -> a, HashMap::new));

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
        String additions = columnNames.entrySet().stream()
            .map(e -> " ADD COLUMN " + e.getKey() + " " + e.getValue().getPostgresType())
            .collect(Collectors.joining(","));
        return "alter table " + schemaName + "." + tableName + additions + ";";
    }

    private boolean parsesWith(DateTimeFormatter formatter, String value) {
        try {
            formatter.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
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
