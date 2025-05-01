package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.utils.Entity;
import egm.io.nifi.processors.ngsild.utils.NGSIConstants.POSTGRESQL_COLUMN_TYPES;
import egm.io.nifi.processors.ngsild.utils.NGSIEvent;
import egm.io.nifi.processors.ngsild.utils.NGSIUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.*;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FlowFileGroup;
import org.apache.nifi.util.db.JdbcCommon;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.nifi.processor.util.pattern.ExceptionHandler.createOnError;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Postgresql", "sql", "put", "rdbms", "database", "create", "insert", "relational", "NGSI-LD", "FIWARE"})
@CapabilityDescription("Create a database if not exists using the information coming from an NGSI-LD event converted to flow file." +
    "After insert all of the vales of the flow file content extraction the entities and attributes")
public class NgsiLdToPostgreSQL extends AbstractSessionFactoryProcessor {

    protected static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
        .name("JDBC Connection Pool")
        .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
            + "The Connection Pool is necessary in order to determine the appropriate database column types.")
        .identifiesControllerService(DBCPService.class)
        .required(true)
        .build();
    protected static final PropertyDescriptor DEFAULT_TENANT = new PropertyDescriptor.Builder()
        .name("default-tenant")
        .displayName("Default NGSI-LD Tenant")
        .description("Default NGSI-LD Tenant for building the database name")
        .required(false)
        .defaultValue("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    protected static final PropertyDescriptor DATASETID_PREFIX_TRUNCATE = new PropertyDescriptor.Builder()
        .name("datasetid-prefix-truncate")
        .displayName("Dataset id prefix to truncate")
        .description("Prefix to truncate from dataset ids when generating column names for multi-attributes")
        .required(false)
        .defaultValue("urn:ngsi-ld:Dataset:")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    protected static final PropertyDescriptor EXPORT_SYSATTRS = new PropertyDescriptor.Builder()
        .name("export-sysattrs")
        .displayName("Export Sysattrs")
        .description("true or false, true for exporting the sys attributes of entities and attributes.")
        .required(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();
    protected static final PropertyDescriptor IGNORE_EMPTY_OBSERVED_AT = new PropertyDescriptor.Builder()
        .name("ignore-empty-observed-at")
        .displayName("Ignore empty observed at lines")
        .description("true or false, true for ignoring rows without any observation date.")
        .required(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();
    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The preferred number of FlowFiles to put to the database in a single transaction")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10")
        .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after the database is successfully updated")
        .build();
    protected static final Relationship REL_RETRY = new Relationship.Builder()
        .name("retry")
        .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
        .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
            + "such as an invalid query or an integrity constraint violation")
        .build();

    protected static final String TABLE_NAME_SUFFIX = "Export-TableNameSuffix";
    protected static final String IGNORED_ATTRIBUTES = "Export-IgnoredAttributes";
    protected static final String FLATTEN_OBSERVATIONS = "Export-FlattenObservations";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(DEFAULT_TENANT);
        properties.add(DATASETID_PREFIX_TRUNCATE);
        properties.add(EXPORT_SYSATTRS);
        properties.add(IGNORE_EMPTY_OBSERVED_AT);
        properties.add(BATCH_SIZE);
        properties.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    /**
     * Extract a list of NGSI-LD Attributes to ignore when processing the given flow file. The list of attributes
     * is conveyed via the {@value IGNORED_ATTRIBUTES} flow file attribute as a comma-separated list of strings.
     */
    private Set<String> getIgnoredAttributes(final FlowFile flowFile) {
        final String ignoredAttributesAttribute = flowFile.getAttribute(IGNORED_ATTRIBUTES);
        if (ignoredAttributesAttribute == null)
            return Collections.emptySet();
        else
            return Arrays.stream(flowFile.getAttribute(IGNORED_ATTRIBUTES).split(",")).collect(Collectors.toSet());
    }

    private static final PostgreSQLBackend postgres = new PostgreSQLBackend();
    private ExceptionHandler<FunctionContext> exceptionHandler;
    private PutGroup<FunctionContext, Connection, StatementFlowFileEnclosure> process;

    private final PartialFunctions.FetchFlowFiles<FunctionContext> fetchFlowFiles = (c, s, fc, r) -> pollFlowFiles(c, s);

    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc, ff) -> {
        final Connection connection = c.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class)
            .getConnection(ff == null ? Collections.emptyMap() : ff.get(0).getAttributes());
        try {
            fc.originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ProcessException("Failed to disable auto commit due to " + e, e);
        }
        return connection;
    };

    private final GroupingFunction groupFlowFilesBySQLBatch = (context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result) -> {
        for (final FlowFile flowFile : flowFiles) {
            NGSIUtils n = new NGSIUtils();
            final boolean flattenObservations = flowFile.getAttribute(FLATTEN_OBSERVATIONS) != null &&
                Objects.equals(flowFile.getAttribute(FLATTEN_OBSERVATIONS), "true");
            final NGSIEvent event = n.getEventFromFlowFile(flowFile, flattenObservations, session);
            final long creationTime = event.getCreationTime();
            final String ngsiLdTenant =
                (event.getNgsiLdTenant().compareToIgnoreCase("") == 0) ?
                    context.getProperty(DEFAULT_TENANT).getValue() : event.getNgsiLdTenant();
            try {
                final String schemaName = postgres.buildSchemaName(ngsiLdTenant);

                List<Entity> entities = event.getEntities();
                for (Entity entity : entities) {
                    getLogger().info("Exporting entity " + entity.entityId);

                    String tableName =
                        postgres.buildTableName(entity, flowFile.getAttribute(TABLE_NAME_SUFFIX).toLowerCase());

                    Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields =
                        postgres.listOfFields(
                            entity,
                            context.getProperty(DATASETID_PREFIX_TRUNCATE).getValue(),
                            context.getProperty(EXPORT_SYSATTRS).asBoolean(),
                            getIgnoredAttributes(flowFile)
                        );

                    ResultSet columnDataType = conn.createStatement().executeQuery(postgres.getColumnsTypes(tableName));
                    Map<String, POSTGRESQL_COLUMN_TYPES> updatedListOfTypedFields;
                    if (columnDataType != null)
                        updatedListOfTypedFields = postgres.getUpdatedListOfTypedFields(columnDataType, listOfFields);
                    else updatedListOfTypedFields = listOfFields;

                    final String sql =
                        postgres.insertQuery(
                            entity,
                            creationTime,
                            schemaName,
                            tableName,
                            updatedListOfTypedFields,
                            context.getProperty(DATASETID_PREFIX_TRUNCATE).getValue(),
                            context.getProperty(EXPORT_SYSATTRS).asBoolean(),
                            context.getProperty(IGNORE_EMPTY_OBSERVED_AT).asBoolean(),
                            flattenObservations
                        );
                    getLogger().debug("Prepared insert query: {}", sql);
                    // Get or create the appropriate PreparedStatement to use.
                    final StatementFlowFileEnclosure enclosure = sqlToEnclosure
                        .computeIfAbsent(sql, k -> {
                            final StatementFlowFileEnclosure newEnclosure = new StatementFlowFileEnclosure(sql);
                            groups.add(newEnclosure);
                            return newEnclosure;
                        });

                    if (!exceptionHandler.execute(fc, flowFile, input -> {
                        final PreparedStatement stmt = enclosure.getCachedStatement(conn);
                        JdbcCommon.setParameters(stmt, flowFile.getAttributes());

                        getLogger().debug("Gonna create schema {}", schemaName);
                        conn.createStatement().execute(postgres.createSchema(schemaName));
                        getLogger().debug("Gonna create table {} with columns {}", tableName, updatedListOfTypedFields);
                        conn.createStatement().execute(postgres.createTable(schemaName, tableName, updatedListOfTypedFields));
                        ResultSet rs = conn.createStatement().executeQuery(postgres.checkColumnNames(tableName));
                        Map<String, POSTGRESQL_COLUMN_TYPES> newColumns = postgres.getNewColumns(rs, updatedListOfTypedFields);
                        if (!newColumns.isEmpty()) {
                            getLogger().debug("Identified new columns to create: {}", newColumns);
                            conn.createStatement().execute(postgres.addColumns(schemaName, tableName, newColumns));
                        }

                        stmt.addBatch();
                    }, onFlowFileError(context, session, result))) {
                        continue;
                    }
                    enclosure.addFlowFile(flowFile);
                }
            } catch (Exception e) {
                getLogger().error("Unexpected exception processing flow file: {}", e.toString(), e);
            }
        }
    };

    private final PutGroup.GroupFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> groupFlowFiles = (context, session, fc, conn, flowFiles, result) -> {
        final Map<String, StatementFlowFileEnclosure> sqlToEnclosure = new HashMap<>();
        final List<StatementFlowFileEnclosure> groups = new ArrayList<>();

        groupFlowFilesBySQLBatch.apply(context, session, fc, conn, flowFiles, groups, sqlToEnclosure, result);

        return groups;
    };

    private final PutGroup.PutFlowFiles<FunctionContext, Connection, StatementFlowFileEnclosure> putFlowFiles = (context, session, fc, conn, enclosure, result) -> {

        // We have PreparedStatement that have batches added to them.
        // We need to execute each batch and close the PreparedStatement.
        exceptionHandler.execute(fc, enclosure, input -> {
            try (final PreparedStatement stmt = enclosure.getCachedStatement(conn)) {
                stmt.executeBatch();
                result.routeTo(enclosure.getFlowFiles(), REL_SUCCESS);
            }
        }, onBatchUpdateError(context, session, result));

        if (result.contains(REL_SUCCESS)) {
            // Determine the database URL
            String url = "jdbc://unknown-host";
            try {
                url = conn.getMetaData().getURL();
            } catch (final SQLException sqle) {
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fc.startNanos);
            for (final FlowFile flowFile : result.getRoutedFlowFiles().get(REL_SUCCESS)) {
                session.getProvenanceReporter().send(flowFile, url, transmissionMillis, true);
            }
        }
    };

    private ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError(
        final ProcessContext context,
        final ProcessSession session,
        final RoutingResult result
    ) {
        ExceptionHandler.OnError<FunctionContext, FlowFile> onFlowFileError = createOnError(context, session, result, REL_FAILURE, REL_RETRY);
        onFlowFileError = onFlowFileError.andThen((c, i, r, e) -> {
            switch (r.destination()) {
                case Failure:
                    logErrorMessage("Failed to update database for {} due to {}; routing to failure", new Object[]{i, e}, e);
                    break;
                case Retry:
                    logErrorMessage(
                        "Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                        new Object[]{i, e},
                        e
                    );
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    private ExceptionHandler.OnError<FunctionContext, StatementFlowFileEnclosure> onBatchUpdateError(
        final ProcessContext context,
        final ProcessSession session,
        final RoutingResult result
    ) {
        return RollbackOnFailure.createOnError((c, enclosure, r, e) -> {

            // If rollbackOnFailure is enabled, the error will be thrown as ProcessException instead.
            if (e instanceof BatchUpdateException && !c.isRollbackOnFailure()) {

                // If we get a BatchUpdateException, then we want to determine which FlowFile caused the failure
                // and route that FlowFile to failure while routing those that finished processing to success and those
                // that have not yet been executed to retry.
                // Currently fragmented transaction does not use batch update.
                final int[] updateCounts = ((BatchUpdateException) e).getUpdateCounts();
                final List<FlowFile> batchFlowFiles = enclosure.getFlowFiles();

                // In the presence of a BatchUpdateException, the driver has the option of either stopping when an error
                // occurs or continuing. If it continues, then it must account for all statements in the batch and for
                // those that fail to return a Statement.EXECUTE_FAILED for the number of rows updated.
                // So we will iterate over all the update counts returned. If any is equal to Statement.EXECUTE_FAILED,
                // we will route the corresponding FlowFile to failure. Otherwise, the FlowFile will go to success
                // unless it has not yet been processed (its index in the List > updateCounts.length).
                int failureCount = 0;
                int successCount = 0;
                int retryCount = 0;
                for (int i = 0; i < updateCounts.length; i++) {
                    final int updateCount = updateCounts[i];
                    final FlowFile flowFile = batchFlowFiles.get(i);
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        result.routeTo(flowFile, REL_FAILURE);
                        failureCount++;
                    } else {
                        result.routeTo(flowFile, REL_SUCCESS);
                        successCount++;
                    }
                }

                if (failureCount == 0) {
                    // if no failures found, the driver decided not to execute the statements after the
                    // failure, so route the last one to failure.
                    final FlowFile failedFlowFile = batchFlowFiles.get(updateCounts.length);
                    result.routeTo(failedFlowFile, REL_FAILURE);
                    failureCount++;
                }

                if (updateCounts.length < batchFlowFiles.size()) {
                    final List<FlowFile> unexecuted = batchFlowFiles.subList(updateCounts.length + 1, batchFlowFiles.size());
                    for (final FlowFile flowFile : unexecuted) {
                        result.routeTo(flowFile, REL_RETRY);
                        retryCount++;
                    }
                }

                logErrorMessage(
                    "Failed to update database due to a failed batch update, {}. There were a total of {} FlowFiles that failed, {} that succeeded, "
                        + "and {} that were not execute and will be routed to retry; ",
                    new Object[]{e, failureCount, successCount, retryCount},
                    e
                );

                return;

            }

            // Apply default error handling and logging for other Exceptions.
            ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError
                = ExceptionHandler.createOnGroupError(context, session, result, REL_FAILURE, REL_RETRY);
            onGroupError = onGroupError.andThen((cl, il, rl, el) -> {
                switch (r.destination()) {
                    case Failure:
                        logErrorMessage(
                            "Failed to update database for {} due to {}; routing to failure",
                            new Object[]{il.getFlowFiles(), e},
                            e
                        );
                        break;
                    case Retry:
                        logErrorMessage(
                            "Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                            new Object[]{il.getFlowFiles(), e},
                            e
                        );
                        break;
                    default:
                        break;
                }
            });
            onGroupError.apply(c, enclosure, r, e);
        });
    }

    @OnScheduled
    public void constructProcess() {
        process = new PutGroup<>();

        process.setLogger(getLogger());
        process.fetchFlowFiles(fetchFlowFiles);
        process.initConnection(initConnection);
        process.groupFetchedFlowFiles(groupFlowFiles);
        process.putFlowFiles(putFlowFiles);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));

        process.onCompleted((c, s, fc, conn) -> {
            try {
                conn.commit();
            } catch (SQLException e) {
                // Throw ProcessException to rollback process session.
                throw new ProcessException("Failed to commit database connection due to " + e, e);
            }
        });

        process.onFailed((c, s, fc, conn, e) -> {
            try {
                conn.rollback();
            } catch (SQLException re) {
                // Log the fact that rollback failed.
                // ProcessSession will be rollbacked by the thrown Exception, so don't have to do anything here.
                getLogger().warn("Failed to rollback database connection due to " + re, re);
            }
        });

        process.cleanup((c, s, fc, conn) -> {
            // make sure that we try to set the auto commit back to whatever it was.
            if (fc.originalAutoCommit) {
                try {
                    conn.setAutoCommit(true);
                } catch (final SQLException se) {
                    getLogger().warn("Failed to reset autocommit due to {}", new Object[]{se});
                }
            }
        });

        exceptionHandler = new ExceptionHandler<>();
        exceptionHandler.mapException(e -> {
            if (e instanceof SQLNonTransientException) {
                return ErrorTypes.InvalidInput;
            } else if (e instanceof SQLException) {
                return ErrorTypes.TemporalFailure;
            } else {
                return ErrorTypes.UnknownFailure;
            }
        });
        BiFunction<FunctionContext, ErrorTypes, ErrorTypes.Result> adjustError = RollbackOnFailure.createAdjustError(getLogger());
        exceptionHandler.adjustError(adjustError);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure);
        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> process.onTrigger(context, session, functionContext));
    }

    /**
     * Pulls a batch of FlowFiles from the incoming queues. If no FlowFiles are available, returns <code>null</code>.
     * Otherwise, a List of FlowFiles will be returned.
     *
     * @param context the process context for determining properties
     * @param session the process session for pulling flowfiles
     * @return a FlowFilePoll containing a List of FlowFiles to process, or <code>null</code> if there are no FlowFiles to process
     */
    private List<FlowFile> pollFlowFiles(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles.isEmpty()) {
            return null;
        }

        return flowFiles;
    }

    @FunctionalInterface
    private interface GroupingFunction {
        void apply(final ProcessContext context, final ProcessSession session, final FunctionContext fc,
            final Connection conn, final List<FlowFile> flowFiles,
            final List<StatementFlowFileEnclosure> groups,
            final Map<String, StatementFlowFileEnclosure> sqlToEnclosure,
            final RoutingResult result);
    }

    private static class FunctionContext extends RollbackOnFailure {
        private final long startNanos = System.nanoTime();
        private boolean originalAutoCommit = false;

        private FunctionContext(boolean rollbackOnFailure) {
            super(rollbackOnFailure, true);
        }
    }

    /**
     * A simple, immutable data structure to hold a Prepared Statement and a List of FlowFiles
     * for which that statement should be evaluated.
     */
    private static class StatementFlowFileEnclosure implements FlowFileGroup {
        private final String sql;
        private final List<FlowFile> flowFiles = new ArrayList<>();
        private PreparedStatement statement;

        public StatementFlowFileEnclosure(String sql) {
            this.sql = sql;
        }

        public PreparedStatement getCachedStatement(final Connection conn) throws SQLException {
            if (statement != null) {
                return statement;
            }

            statement = conn.prepareStatement(sql);
            return statement;
        }

        @Override
        public List<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public void addFlowFile(final FlowFile flowFile) {
            this.flowFiles.add(flowFile);
        }

        @Override
        public int hashCode() {
            return sql.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return false;
            }
            if (!(obj instanceof StatementFlowFileEnclosure)) {
                return false;
            }

            final StatementFlowFileEnclosure other = (StatementFlowFileEnclosure) obj;
            return sql.equals(other.sql);
        }
    }

    private void logErrorMessage(final String message, Object[] objects, final Throwable t) {
        getLogger().error(
            new LogMessage.Builder(System.currentTimeMillis(), LogLevel.ERROR)
                .message(message)
                .objects(objects)
                .throwable(t)
                .createLogMessage()
        );
    }
}
