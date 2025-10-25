package egm.io.nifi.processors.ngsild;

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.*;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.ERROR_MESSAGE_ATTR;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.IGNORE_EMPTY_OBSERVED_AT;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.TABLE_NAME_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class TestNgsiLdToPostgreSQL {

    @Container
    private static final PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>(
        DockerImageName.parse("postgis/postgis:16-3.5-alpine").asCompatibleSubstituteFor("postgres")
    );

    private TestRunner runner;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NgsiLdToPostgreSQL.class);

        runner.setProperty(NgsiLdToPostgreSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(NgsiLdToPostgreSQL.BATCH_SIZE, "100");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        runner.setProperty(DB_SCHEMA, "public");

        DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("dbcp", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, postgreSQLContainer.getJdbcUrl() + "/" + postgreSQLContainer.getDatabaseName());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, postgreSQLContainer.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, postgreSQLContainer.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, postgreSQLContainer.getDriverClassName());
        runner.enableControllerService(connectionPool);
        runner.assertValid(connectionPool);
    }

    public void checkColumns(
        Connection connection,
        String schema,
        String tableName,
        List<String> expectedColumns
    ) throws SQLException {
        Set<String> expectedColumnsSet = new HashSet<>(expectedColumns);

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, schema, tableName, null);

        Set<String> actualColumnsSet = new HashSet<>();
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            actualColumnsSet.add(columnName);
        }
        columns.close();

        assertEquals(expectedColumnsSet, actualColumnsSet, "The columns in the table do not match the expected columns.");
    }

    public void checkRowCount(
        Connection connection,
        String schema,
        String tableName,
        int expectedRowCount
    ) throws SQLException {
        String query = String.format("SELECT COUNT(*) FROM %s.%s", schema, tableName);
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(query);

            resultSet.next();
            int actualRowCount = resultSet.getInt(1);

            assertEquals(expectedRowCount, actualRowCount, "The number of rows in the table do not match the expected number of rows.");
        }
    }

    public void checkColumnValue(
        Connection connection,
        String schema,
        String tableName,
        int rowNumber,
        String entityId,
        String columnName,
        String expectedColumnValue
    ) throws SQLException {
        String query = String.format(
            "SELECT %s FROM %s.%s WHERE entityid = '%s'",
            columnName,
            schema,
            tableName,
            entityId
        );
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.getRow() < rowNumber) resultSet.next();
            String actualColumnValue = resultSet.getString(columnName);

            assertEquals(expectedColumnValue, actualColumnValue, "The actual value of the column does not match the expected value.");
        }
    }

    @Test
    public void currentStateDefaultExport() throws IOException , SQLException{
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 1;
        List<String> expectedColumns = Arrays.asList(
                "addresslocality",
                "containedin",
                "concessionnumber",
                "entityid",
                "entitytype",
                "expirydate",
                "familyofuse",
                "location",
                "location_geometry",
                "location_geojson",
                "managementstructure",
                "natureofuselabel",
                "parcel",
                "rank",
                "structure",
                "surface",
                "surface_unitcode",
                "recvtime"
        );


        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "shellfishtable", expectedRowCount);
            checkColumns(connection, "public", "shellfishtable", expectedColumns);
        }
        finally {
            dropTable("public", "shellfishtable");
        }
    }

    @Test
    public void currentStateDefaultExportBatch() throws IOException , SQLException{
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 3);

        int expectedRowCount = 3;
        List<String> expectedColumns = Arrays.asList(
                "addresslocality",
                "containedin",
                "concessionnumber",
                "entityid",
                "entitytype",
                "expirydate",
                "familyofuse",
                "location",
                "location_geometry",
                "location_geojson",
                "managementstructure",
                "natureofuselabel",
                "parcel",
                "rank",
                "structure",
                "surface",
                "surface_unitcode",
                "recvtime"
        );


        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "shellfishtable", expectedRowCount);
            checkColumns(connection, "public", "shellfishtable", expectedColumns);
        }
        finally {
            dropTable("public", "shellfishtable");
        }
    }

    @Test
    public void currentStateDefaultExportSchemaAndTableNameSuffix() throws IOException , SQLException{
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.setProperty(DB_SCHEMA, "private");
        runner.setProperty(TABLE_NAME_SUFFIX, "suffix");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 1;
        List<String> expectedColumns = Arrays.asList(
                "addresslocality",
                "containedin",
                "concessionnumber",
                "entityid",
                "entitytype",
                "expirydate",
                "familyofuse",
                "location",
                "location_geometry",
                "location_geojson",
                "managementstructure",
                "natureofuselabel",
                "parcel",
                "rank",
                "structure",
                "surface",
                "surface_unitcode",
                "recvtime"
        );


        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "private", "shellfishtable_suffix", expectedRowCount);
            checkColumns(connection, "private", "shellfishtable_suffix", expectedColumns);
        }
        finally {
            dropTable("private", "shellfishtable_suffix");
        }
    }

    @Test
    public void currentStateFlattenExport() throws IOException , SQLException{
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.setProperty(FLATTEN_OBSERVATIONS, "true");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 0;
        List<String> expectedColumns = Arrays.asList(
                "addresslocality",
                "containedin",
                "concessionnumber",
                "entityid",
                "entitytype",
                "expirydate",
                "familyofuse",
                "location",
                "location_geometry",
                "location_geojson",
                "managementstructure",
                "natureofuselabel",
                "parcel",
                "rank",
                "structure",
                "surface",
                "surface_unitcode",
                "recvtime"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "shellfishtable", expectedRowCount);
            checkColumns(connection, "public", "shellfishtable", expectedColumns);
        }
        finally {
            dropTable("public", "shellfishtable");
        }
    }

    @Test
    public void notificationDefaultExport() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.enqueue(loadTestFile("entity-notification.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "entityid",
                "entitytype",
                "faecalcoliform_k63_0to1meter",
                "faecalcoliform_k63_0to1meter_citation",
                "faecalcoliform_k63_0to1meter_depthsampling",
                "faecalcoliform_k63_0to1meter_ispartofprogram",
                "faecalcoliform_k63_0to1meter_qualitydescription",
                "faecalcoliform_k63_0to1meter_qualitylevel",
                "faecalcoliform_k63_0to1meter_sampledescription",
                "faecalcoliform_k63_0to1meter_unitcode",
                "faecalcoliform_k63_halfbottom",
                "faecalcoliform_k63_halfbottom_citation",
                "faecalcoliform_k63_halfbottom_depthsampling",
                "faecalcoliform_k63_halfbottom_ispartofprogram",
                "faecalcoliform_k63_halfbottom_qualitydescription",
                "faecalcoliform_k63_halfbottom_qualitylevel",
                "faecalcoliform_k63_halfbottom_sampledescription",
                "faecalcoliform_k63_halfbottom_unitcode",
                "observedat",
                "recvtime",
                "sextantcode",
                "servesdataset",
                "servesdataset_catalog",
                "servesdataset_description",
                "servesdataset_group",
                "servesdataset_includedparameters",
                "servesdataset_ispublishedby",
                "servesdataset_landingpage",
                "servesdataset_specificaccesspolicy",
                "servesdataset_subtheme",
                "servesdataset_title",
                "stationcode",
                "specificaccesspolicy",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void notificationDefaultExportIgnoredAttributesAndDatasetTruncate() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.setProperty(IGNORED_ATTRIBUTES, "servesdataset,unitcode,citation");
        runner.setProperty(DATASETID_PREFIX_TRUNCATE, "urn:ngsi-ld:");
        runner.enqueue(loadTestFile("entity-notification.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "entityid",
                "entitytype",
                "faecalcoliform_dataset_k63_0to1meter",
                "faecalcoliform_dataset_k63_0to1meter_depthsampling",
                "faecalcoliform_dataset_k63_0to1meter_ispartofprogram",
                "faecalcoliform_dataset_k63_0to1meter_qualitydescription",
                "faecalcoliform_dataset_k63_0to1meter_qualitylevel",
                "faecalcoliform_dataset_k63_0to1meter_sampledescription",
                "faecalcoliform_dataset_k63_0to1meter_qualitylevel",
                "faecalcoliform_dataset_k63_halfbottom",
                "faecalcoliform_dataset_k63_halfbottom_depthsampling",
                "faecalcoliform_dataset_k63_halfbottom_ispartofprogram",
                "faecalcoliform_dataset_k63_halfbottom_qualitydescription",
                "faecalcoliform_dataset_k63_halfbottom_qualitylevel",
                "faecalcoliform_dataset_k63_halfbottom_sampledescription",
                "observedat",
                "recvtime",
                "sextantcode",
                "specificaccesspolicy",
                "stationcode",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void notificationFlattenExport() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "true");
        runner.enqueue(loadTestFile("entity-notification.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "entityid",
                "entitytype",
                "measure",
                "measure_citation",
                "measure_datasetid",
                "measure_depthsampling",
                "measure_ispartofprogram",
                "measure_observedat",
                "measure_parametername",
                "measure_qualitydescription",
                "measure_qualitylevel",
                "measure_sampledescription",
                "measure_unitcode",
                "recvtime",
                "sextantcode",
                "servesdataset",
                "servesdataset_catalog",
                "servesdataset_description",
                "servesdataset_group",
                "servesdataset_includedparameters",
                "servesdataset_landingpage",
                "servesdataset_specificaccesspolicy",
                "servesdataset_subtheme",
                "servesdataset_title",
                "servesdataset_ispublishedby",
                "servesdataset_title",
                "stationcode",
                "specificaccesspolicy",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
            for (int rowNumber = 1; rowNumber <= 2; rowNumber++) {
                checkColumnValue(
                    connection,
                    "public",
                    "distribution",
                    rowNumber,
                    "urn:ngsi-ld:Distribution:Zone:104-P-006:001",
                    "stationcode",
                    "104-P-006"
                );
                checkColumnValue(
                    connection,
                    "public",
                    "distribution",
                    rowNumber,
                    "urn:ngsi-ld:Distribution:Zone:104-P-006:001",
                    "servesdataset_title",
                    "Surveillance littorale (Microbiologie - Microbiologie/Bactéries tests)"
                );
            }
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void temporalDefaultExport() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.enqueue(loadTestFile("entity-temporal.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "accessurl",
                "entityid",
                "entitytype",
                "faecalcoliform",
                "faecalcoliform_unitcode",
                "faecalenterococcus",
                "faecalenterococcus_unitcode",
                "lastmodifiedat",
                "location",
                "location_geojson",
                "location_geometry",
                "location_lat",
                "location_lon",
                "observedat",
                "recvtime",
                "servesdataset",
                "servesdataset_catalog",
                "servesdataset_description",
                "servesdataset_includedparameters",
                "servesdataset_landingpage",
                "servesdataset_theme",
                "servesdataset_title",
                "specificaccesspolicy",
                "stationcode",
                "status",
                "temporalresolution",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void temporalDefaultExportExportSysAttrs() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.setProperty(EXPORT_SYSATTRS, "true");
        runner.enqueue(loadTestFile("entity-temporal.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "accessurl",
                "accessurl_createdat",
                "accessurl_modifiedat",
                "entityid",
                "entitytype",
                "faecalcoliform",
                "faecalcoliform_unitcode",
                "faecalenterococcus",
                "faecalenterococcus_unitcode",
                "lastmodifiedat",
                "lastmodifiedat_createdat",
                "lastmodifiedat_modifiedat",
                "location",
                "location_createdat",
                "location_geojson",
                "location_geometry",
                "location_lat",
                "location_lon",
                "location_modifiedat",
                "observedat",
                "recvtime",
                "servesdataset",
                "servesdataset_catalog",
                "servesdataset_createdat",
                "servesdataset_description",
                "servesdataset_includedparameters",
                "servesdataset_landingpage",
                "servesdataset_modifiedat",
                "servesdataset_theme",
                "servesdataset_title",
                "stationcode",
                "stationcode_createdat",
                "stationcode_modifiedat",
                "specificaccesspolicy",
                "specificaccesspolicy_createdat",
                "specificaccesspolicy_modifiedat",
                "status",
                "status_createdat",
                "status_modifiedat",
                "temporalresolution",
                "temporalresolution_createdat",
                "temporalresolution_modifiedat",
                "title",
                "title_createdat",
                "title_modifiedat"
        );



        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void temporalDefaultExportIgnoredAttributes() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "false");
        runner.setProperty(IGNORED_ATTRIBUTES, "servesdataset,unitcode");
        runner.enqueue(loadTestFile("entity-temporal.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 2;
        List<String> expectedColumns = Arrays.asList(
                "accessurl",
                "entityid",
                "entitytype",
                "faecalcoliform",
                "faecalenterococcus",
                "lastmodifiedat",
                "location",
                "location_geojson",
                "location_geometry",
                "location_lat",
                "location_lon",
                "observedat",
                "recvtime",
                "specificaccesspolicy",
                "stationcode",
                "status",
                "temporalresolution",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
        }
        finally {
            dropTable("public", "distribution");
        }
    }


    @Test
    public void temporalFlattenExport() throws IOException , SQLException{
        runner.setProperty(FLATTEN_OBSERVATIONS, "true");
        runner.enqueue(loadTestFile("entity-temporal.jsonld").getBytes(), Collections.emptyMap());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        int expectedRowCount = 4;
        List<String> expectedColumns = Arrays.asList(
                "accessurl",
                "entityid",
                "entitytype",
                "lastmodifiedat",
                "location",
                "location_geojson",
                "location_geometry",
                "location_lat",
                "location_lon",
                "measure",
                "measure_datasetid",
                "measure_observedat",
                "measure_parametername",
                "measure_unitcode",
                "recvtime",
                "servesdataset",
                "servesdataset_catalog",
                "servesdataset_description",
                "servesdataset_includedparameters",
                "servesdataset_landingpage",
                "servesdataset_theme",
                "servesdataset_title",
                "specificaccesspolicy",
                "stationcode",
                "status",
                "temporalresolution",
                "title"
        );

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            checkRowCount(connection, "public", "distribution", expectedRowCount);
            checkColumns(connection, "public", "distribution", expectedColumns);
            for (int rowNumber = 1; rowNumber <= 4; rowNumber++) {
                checkColumnValue(
                    connection,
                    "public",
                    "distribution",
                    rowNumber,
                    "urn:ngsi-ld:Distribution:MicrobiologieDREAL:P6",
                    "stationcode",
                    "P6"
                );
                checkColumnValue(
                    connection,
                    "public",
                    "distribution",
                    rowNumber,
                    "urn:ngsi-ld:Distribution:MicrobiologieDREAL:P6",
                    "servesdataset_title",
                    "Microbiologie DREAL - SMBT"
                );
            }
        }
        finally {
            dropTable("public", "distribution");
        }
    }

    @Test
    public void itShouldRouteToFailureAndAddErrorAttributeIfSchemNameIsTooLong() throws IOException {
        runner.setProperty(
            DB_SCHEMA,
            "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongSchema"
        );
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.assertAllConditionsMet(REL_FAILURE, mff ->
            mff.isAttributeEqual(
                ERROR_MESSAGE_ATTR,
                "Building schema name 'tooloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongschema' and its length is greater than 63"
            )
        );
    }

    @Test
    public void itShouldRouteToFailureAndAddErrorAttributeIfInvalidSchemName() throws IOException {
        runner.setProperty(DB_SCHEMA, "default");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), Collections.emptyMap());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.assertAllConditionsMet(REL_FAILURE, mff -> {
            mff.assertAttributeExists(ERROR_MESSAGE_ATTR);
            mff.assertAttributeEquals(ERROR_SQL_STATE_ATTR, "42601");
            return true;
        });
    }

    @Test
    public void itShouldRouteToFailureAndAddErrorAttributeIfTableNameIsTooLong() throws IOException {
        runner.setProperty(TABLE_NAME_SUFFIX, "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongSuffix");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes());

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        runner.assertAllConditionsMet(REL_FAILURE, mff ->
            mff.isAttributeEqual(
                ERROR_MESSAGE_ATTR,
                "Building table name 'shellfishtable_tooloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongsuffix' and its length is greater than 63"
            )
        );
    }

    @Test
    public void itShouldRouteToFailureAndSuccessIfTableNameIsTooLongForOneFlowFile() throws IOException {
        runner.setProperty(TABLE_NAME_SUFFIX, "${tableNameSuffix}");
        runner.enqueue(
            loadTestFile("entity-current.jsonld").getBytes(),
            Collections.singletonMap(
                "tableNameSuffix",
                "tooloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongsuffix"
            )
        );
        runner.enqueue(
            loadTestFile("entity-current.jsonld").getBytes(),
            Collections.singletonMap("tableNameSuffix", "")
        );

        runner.run();

        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 1);
        runner.assertAllConditionsMet(REL_FAILURE, mff ->
            mff.isAttributeEqual(
                ERROR_MESSAGE_ATTR,
                "Building table name 'shellfishtable_tooloooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongsuffix' and its length is greater than 63"
            )
        );
    }

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }

    private void dropTable(String schemaName, String tableName) throws ProcessException, SQLException {
        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            try (final Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("drop table " + schemaName + "." + tableName);
            }
        }
    }
}
