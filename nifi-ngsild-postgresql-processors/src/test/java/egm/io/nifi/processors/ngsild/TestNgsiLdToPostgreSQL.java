package egm.io.nifi.processors.ngsild;

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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
    public void itShouldExportCurrentStateOfEntity() throws IOException {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(NgsiLdToPostgreSQL.TABLE_NAME_SUFFIX, "");
        runner.setProperty(NgsiLdToPostgreSQL.IGNORE_EMPTY_OBSERVED_AT, "false");

        runner.enqueue(loadTestFile("entity-current.jsonld").getBytes(), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred(NgsiLdToPostgreSQL.REL_SUCCESS, 1);

        try {
            ResultSet result = postgreSQLContainer.createConnection("")
                .createStatement()
                .executeQuery("SELECT count(*) FROM public.shellfishtable");
            result.next();
            assertEquals(1, result.getInt(1));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        runner.assertAllConditionsMet("success",
            mff -> mff.isAttributeEqual(NgsiLdToPostgreSQL.TABLE_NAME_SUFFIX, "")
        );
    }

    private String loadTestFile(String filename) throws IOException {
        return Files.readString(Paths.get("src/test/resources/" + filename));
    }
}
