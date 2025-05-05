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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.*;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.ERROR_MESSAGE_ATTR;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.IGNORE_EMPTY_OBSERVED_AT;
import static egm.io.nifi.processors.ngsild.NgsiLdToPostgreSQL.TABLE_NAME_SUFFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class NgsiLdToPostgreSQLTests {

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
        runner.setProperty(NgsiLdToPostgreSQL.DEFAULT_TENANT, "public");

        DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("dbcp", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, postgreSQLContainer.getJdbcUrl() + "/" + postgreSQLContainer.getDatabaseName());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, postgreSQLContainer.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, postgreSQLContainer.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, postgreSQLContainer.getDriverClassName());
        runner.enableControllerService(connectionPool);
        runner.assertValid(connectionPool);
    }

    @Test
    public void itShouldExportCurrentStateOfEntity() throws IOException, SQLException {
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), getDefaultAttributes());

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            try (final Statement statement = connection.createStatement() ) {
                ResultSet result = statement.executeQuery("SELECT count(*) FROM public.shellfishtable");
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
            }
        }

        dropTable("public", "shellfishtable");
    }

    @Test
    public void itShouldExportCurrentStateOfBatchOfEntities() throws IOException, SQLException {
        final Map<String, String> attributes = getDefaultAttributes();
        runner.setProperty(IGNORE_EMPTY_OBSERVED_AT, "false");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), attributes);
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), attributes);
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 3);

        try (final Connection connection = postgreSQLContainer.createConnection("")) {
            try (final Statement statement = connection.createStatement() ) {
                ResultSet result = statement.executeQuery("SELECT count(*) FROM public.shellfishtable");
                assertTrue(result.next());
                assertEquals(3, result.getInt(1));
            }
        }

        dropTable("public", "shellfishtable");
    }

    @Test
    public void itShouldRouteToFailureAndAddErrorAttributeIfSchemNameIsTooLong() throws IOException {
        runner.setProperty(
            DEFAULT_TENANT,
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
        runner.setProperty(DEFAULT_TENANT, "default");
        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), getDefaultAttributes());

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
        runner.enqueue(
            loadTestFile("entity-current.jsonld").getBytes(),
            Collections.singletonMap(
                TABLE_NAME_SUFFIX,
                "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongSuffix"
            )
        );

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
        runner.enqueue(
            loadTestFile("entity-current.jsonld").getBytes(),
            Collections.singletonMap(
                TABLE_NAME_SUFFIX,
                "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongSuffix"
            )
        );
        runner.enqueue(
            loadTestFile("entity-current.jsonld").getBytes(),
            Collections.singletonMap(TABLE_NAME_SUFFIX, "")
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

    private Map<String, String> getDefaultAttributes() {
        return Collections.singletonMap(TABLE_NAME_SUFFIX, "");
    }
}
