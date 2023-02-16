package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.utils.Attributes;
import egm.io.nifi.processors.ngsild.utils.Entity;
import egm.io.nifi.processors.ngsild.utils.NGSIConstants;
import egm.io.nifi.processors.ngsild.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class TestNGSIToPostgreSQL {
    private TestRunner runner;
    private PostgreSQLBackend backend;

    @Before
    public void setUp() {
        //Mock the DBCP Controller Service, so we can control the Results
        runner = TestRunners.newTestRunner(NGSIToPostgreSQL.class);
        runner.setProperty(NGSIToPostgreSQL.CONNECTION_POOL, "dbcp");
        runner.setProperty(NGSIToPostgreSQL.DATA_MODEL, "db-by-entity-type");
        runner.setProperty(NGSIToPostgreSQL.ENABLE_ENCODING, "false");
        runner.setProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE, "false");
        runner.setProperty(NGSIToPostgreSQL.BATCH_SIZE, "100");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        backend = new PostgreSQLBackend();
    }

    /**
     * [NGSIToPostgreSQL.buildDBName] -------- The schema name is equals to the encoding of the notified/defaulted
     * service.
     */
    @Test
    public void testBuildDBNameOldEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildDBName]"
                + "-------- The schema name is equals to the encoding of the notified/defaulted service");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE).asBoolean(); // default
        String service = "someService";

        try {
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase);
            String expectedDBName = "someService";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameOldEncoding

    /**
     * [NGSIToPostgreSQL.buildDBName] -------- The schema name is equals to the encoding of the notified/defaulted
     * service.
     */
    @Test
    public void testBuildDBNameNewEncoding() throws Exception {
        System.out.println("[NGSIToPostgreSQL.buildDBName]"
                + "-------- The schema name is equals to the encoding of the notified/defaulted service");

        String service = "someService";
        runner.setProperty(NGSIToPostgreSQL.ENABLE_ENCODING, "true");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE).asBoolean();


        try {
            String builtSchemaName = backend.buildSchemaName(service,enableEncoding,enableLowercase);
            String expectedDBName = "somex0053ervice";

            try {
                assertEquals(expectedDBName, builtSchemaName);
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "-  OK  - '" + expectedDBName + "' is equals to the encoding of <service>");
            } catch (AssertionError e) {
                System.out.println("[NGSIToPostgreSQL.buildDBName]"
                        + "- FAIL - '" + expectedDBName + "' is not equals to the encoding of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildDBName]"
                    + "- FAIL - There was some problem when building the DB name");
            throw e;
        } // try catch
    } // testBuildDBNameNewEncoding

    /**
     * [NGSIToPostgreSQL.buildSchemaName] -------- A schema name length greater than 63 characters is detected.
     */
    @Test
    public void testBuildSchemaNameLength() {
        System.out.println("[NGSIToPostgreSQL.buildSchemaName]"
                + "-------- A schema name length greater than 63 characters is detected");

        runner.setProperty(NGSIToPostgreSQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE).asBoolean();
        String service = "tooLoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongService";

        try {
            backend.buildSchemaName(service,enableEncoding,enableLowercase);
            fail("[NGSIToPostgreSQL.buildSchemaName]"
                    + "- FAIL - A schema name length greater than 63 characters has not been detected");
        } catch (Exception e) {
            System.out.println("[NGSIToPostgreSQL.buildSchemaName]"
                    + "-  OK  - A schema name length greater than 63 characters has been detected");
        } // try catch
    } // testBuildSchemaNameLength

    /**
     * [NGSICartoDBSink.buildTableName] -------- When data model is by entity, a table name length greater than 63
     * characters is detected.
     */
    @Test
    public void testBuildTableNameLengthDataModelByEntity() {
        System.out.println("[NGSIToPostgreSQL.buildTableName]"
                + "-------- When data model is by entity, a table name length greater than 63 characters is truncated");

        runner.setProperty(NGSIToPostgreSQL.DATA_MODEL, "db-by-entity");
        runner.setProperty(NGSIToPostgreSQL.ENABLE_ENCODING, "false");
        Boolean enableEncoding = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_ENCODING).asBoolean();
        Boolean enableLowercase = runner.getProcessContext().getProperty(NGSIToPostgreSQL.ENABLE_LOWERCASE).asBoolean();
        String dataModel = runner.getProcessContext().getProperty(NGSIToPostgreSQL.DATA_MODEL).getValue();
        Entity entity = new Entity("tooLooooooooooooooooooooooooooooooooooooooooooooooongEntity", "someType",null);

        try {
            String tableName = backend.buildTableName(entity,dataModel,enableEncoding,enableLowercase,null);
            assertTrue(tableName.length() < 63);
        } catch (Exception e) {
            fail("[NGSIToPostgreSQL.buildTableName]"
                    + "- FAIL - A table name length greater than 63 characters has not been detected");
        }
    }

    @Test
    public void testRowFields() {
        System.out.println("[PostgreSQLBackend.listOfFields ]"
                + "-------- When attrPersistence is column");

        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", entityAttrs);

        try {
            Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(entity,"");
            List<String> expList = Arrays.asList("entityId", "entityType", "recvTime", "someattr_urn_ngsi_ld_dataset_01", "someattr_urn_ngsi_ld_dataset_01_observedat");
            Set<String> expectedListOfFields = new HashSet<>(expList);

            try {
                assertEquals(expectedListOfFields, listOfFields.keySet());
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "-  OK  - '" + listOfFields + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.listOfFields]"
                        + "- FAIL - '" + listOfFields + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.listOfFields]"
                    + "- FAIL - There was some problem when building the list of fields");
            throw e;
        } // try catch

    } // testRowFields

    @Test
    public void testValuesForInsertRowWithMetadata() {
        System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                + "-------- When attrPersistence is column");

        ArrayList<Attributes> entityAttrs = new ArrayList<>();
        entityAttrs.add(new Attributes("someAttr", "Property", "urn:ngsi-ld:Dataset:01", "2023-02-16T00:00:00Z", null, null, 12.0, false, new ArrayList<>()));
        Entity entity = new Entity("someId", "someType", entityAttrs);
        long creationTime = 1562561734983L;

        try {
            Map<String, NGSIConstants.POSTGRESQL_COLUMN_TYPES> listOfFields = backend.listOfFields(entity, "");
            List<String> valuesForInsert = backend.getValuesForInsert(entity, listOfFields, creationTime, "");
            List<String> expectedValuesForInsert = List.of("('someId','someType','2019-07-08T04:55:34.983Z',12.0,'2023-02-16T00:00:00Z')");
           
            try {
                assertEquals(expectedValuesForInsert, valuesForInsert);
                System.out.println("[PostgreSQLBackend.getValuesForInsert]"
                        + "-  OK  - '" + valuesForInsert + "' is equals to the expected output");
            } catch (AssertionError e) {
                System.out.println("[PostgreSQLBackend.valuesForInsert]"
                        + "- FAIL - '" + valuesForInsert + "' is not equals to the expected output");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println("[PostgreSQLBackend.valuesForInsert]"
                    + "- FAIL - There was some problem when building values for insert");
            throw e;
        } // try catch

    } // testValuesForInsertRowWithMetadata

    @Test
    public void shouldChangeTheTypeOfField() throws Exception {
        ResultSet resultSetMock = Mockito.mock(ResultSet.class);
        when(resultSetMock.getString(1)).thenReturn("temperature");
        when(resultSetMock.getString(2)).thenReturn("numeric");
        when(resultSetMock.next()).thenReturn(true).thenReturn(false);

        Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields = new TreeMap<>();
        listOfFields.put("temperature", POSTGRESQL_COLUMN_TYPES.TEXT);

        listOfFields = backend.getUpdatedListOfTypedFields(resultSetMock, listOfFields);

        assertEquals(POSTGRESQL_COLUMN_TYPES.NUMERIC, listOfFields.get("temperature"));
        assertNotEquals(POSTGRESQL_COLUMN_TYPES.TEXT, listOfFields.get("temperature"));
    }
}
