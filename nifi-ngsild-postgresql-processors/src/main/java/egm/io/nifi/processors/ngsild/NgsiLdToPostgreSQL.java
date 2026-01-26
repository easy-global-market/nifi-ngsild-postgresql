package egm.io.nifi.processors.ngsild;

import egm.io.nifi.processors.ngsild.PostgreSQLTransformer.POSTGRESQL_COLUMN_TYPES;
import egm.io.nifi.processors.ngsild.model.Entity;
import egm.io.nifi.processors.ngsild.model.Event;
import egm.io.nifi.processors.ngsild.model.ExportMode;
import egm.io.nifi.processors.ngsild.utils.NgsiLdUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;
import org.apache.nifi.migration.PropertyConfiguration;
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

import static java.lang.String.valueOf;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.nifi.processor.util.pattern.ExceptionHandler.createOnError;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Postgresql", "sql", "put", "rdbms", "database", "create", "insert", "relational", "NGSI-LD", "FIWARE"})
@CapabilityDescription("Create a database if not exists using the information coming from an NGSI-LD event converted to flow file." +
    "After insert all of the vales of the flow file content extraction the entities and attributes")
@WritesAttributes({
    @WritesAttribute(
        attribute = "error.message",
        description = "If processing of the flow file fails, this attribute will contain the error message"
    ),
    @WritesAttribute(
        attribute = "error.code",
        description = "If processing of the flow file fails, this attribute will contain the error code (if greater than 0)"
    ),
    @WritesAttribute(
        attribute = "error.sql.state",
        description = "If processing of the flow file fails, this attribute will contain the SQL state (if not null)"
    )
})
public class NgsiLdToPostgreSQL extends AbstractSessionFactoryProcessor {

    protected static final String DEFAULT_DB_SCHEMA = "stellio";

    protected static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
        .name("connection-pool")
        .displayName("JDBC Connection Pool")
        .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
            + "The Connection Pool is necessary in order to determine the appropriate database column types.")
        .identifiesControllerService(DBCPService.class)
        .required(true)
        .build();
    protected static final PropertyDescriptor DB_SCHEMA = new PropertyDescriptor.Builder()
        .name("db-schema")
        .displayName("DB schema")
        .description("DB schema used to create tables")
        .required(false)
        .defaultValue(DEFAULT_DB_SCHEMA)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    protected static final PropertyDescriptor TABLE_NAME_SUFFIX = new PropertyDescriptor.Builder()
        .name("table-name-suffix")
        .displayName("Table name suffix")
        .description("Suffix to add to the name of the created table")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    protected static final PropertyDescriptor EXPORT_MODE = new PropertyDescriptor.Builder()
        .name("export-mode")
        .displayName("Export mode")
        .description("The mode used to export NGSI-LD attributes to database columns. " +
            "Expanded: one column per attribute; " +
            "Flatten: generic columns for all observations; " +
            "Flatten On Multi-Instances Attributes : one column per attribute name with an associated datasetId column.")
        .required(true)
        .allowableValues(ExportMode.class)
        .defaultValue(ExportMode.EXPANDED)
        .build();
    protected static final PropertyDescriptor IGNORE_EMPTY_OBSERVED_AT = new PropertyDescriptor.Builder()
        .name("ignore-empty-observed-at")
        .displayName("Ignore empty observed at lines")
        .description("true or false, true for ignoring rows without any observation date.")
        .required(false)
        .defaultValue("true")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    protected static final PropertyDescriptor REPLACE_MODE = new PropertyDescriptor.Builder()
        .name("replace-mode")
        .displayName("Replace mode")
        .description("true or false, true for deleting all existing data for the received entity IDs before " +
                "inserting the new data.")
        .required(false)
        .defaultValue("false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();
    protected static final PropertyDescriptor IGNORED_ATTRIBUTES = new PropertyDescriptor.Builder()
        .name("ignored-attributes")
        .displayName("Attributes to ignore")
        .description("Attributes to ignore when exporting the entities. Comma-separated list of attribute names.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("batch-size")
        .displayName("Batch Size")
        .description("The preferred number of FlowFiles to put to the database in a single transaction")
        .required(true)
        .defaultValue("10")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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

    protected static final String ERROR_MESSAGE_ATTR = "error.message";
    protected static final String ERROR_CODE_ATTR = "error.code";
    protected static final String ERROR_SQL_STATE_ATTR = "error.sql.state";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(DB_SCHEMA);
        properties.add(TABLE_NAME_SUFFIX);
        properties.add(EXPORT_MODE);
        properties.add(IGNORE_EMPTY_OBSERVED_AT);
        properties.add(REPLACE_MODE);
        properties.add(DATASETID_PREFIX_TRUNCATE);
        properties.add(EXPORT_SYSATTRS);
        properties.add(IGNORED_ATTRIBUTES);
        properties.add(BATCH_SIZE);
        properties.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        return properties;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        final Optional<String> flattenObsRaw = config.getRawPropertyValue("flatten-observations");
        if (flattenObsRaw.isEmpty()) {
            return;
        }
        switch (flattenObsRaw.get()) {
            case "true":
                config.setProperty("export-mode", ExportMode.FLATTEN.name());
                break;
            case "false":
                config.setProperty("export-mode", ExportMode.EXPANDED.name());
                break;
            default:
                break;
        }
        config.removeProperty("flatten-observations");
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
     * is configured via the "ignored-attributes" processor property.
     */
    private Set<String> getIgnoredAttributes(final ProcessContext context, final FlowFile flowFile) {
        final String ignoredAttributes = context.getProperty(IGNORED_ATTRIBUTES).evaluateAttributeExpressions(flowFile).getValue();
        if (ignoredAttributes == null)
            return Collections.emptySet();
        else
            return Arrays.stream(ignoredAttributes.split(",")).collect(Collectors.toSet());
    }

    private static final PostgreSQLTransformer postgres = new PostgreSQLTransformer();
    private ExceptionHandler<FunctionContext> exceptionHandler;
    private PutGroup<FunctionContext, Connection, StatementFlowFileEnclosure> process;

    private final PartialFunctions.FetchFlowFiles<FunctionContext> fetchFlowFiles = (c, s, fc, r) -> pollFlowFiles(c, s);

    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc, ff) -> {
        final Connection connection = c.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class)
            .getConnection(ff == null ? Collections.emptyMap() : ff.getFirst().getAttributes());
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
            String dbSchema = context.getProperty(DB_SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
            if (dbSchema == null || dbSchema.isEmpty())
                dbSchema = DEFAULT_DB_SCHEMA;
            final String tableNameSuffix = context.getProperty(TABLE_NAME_SUFFIX).evaluateAttributeExpressions(flowFile).getValue();
            final ExportMode exportMode = ExportMode.valueOf(context.getProperty(EXPORT_MODE).getValue());
            final boolean ignoreEmptyObservedAt = context.getProperty(IGNORE_EMPTY_OBSERVED_AT).evaluateAttributeExpressions(flowFile).asBoolean();
            final boolean replaceMode = context.getProperty(REPLACE_MODE).evaluateAttributeExpressions(flowFile).asBoolean();
            final Event event = NgsiLdUtils.getEventFromFlowFile(flowFile, exportMode, session);
            final long creationTime = event.getCreationTime();
            try {
                final String schemaName = postgres.buildSchemaName(dbSchema);

                List<Entity> entities = event.getEntities();
                for (Entity entity : entities) {
                    getLogger().info("Exporting entity " + entity.entityId);

                    String tableName = postgres.buildTableName(entity, tableNameSuffix);

                    Map<String, POSTGRESQL_COLUMN_TYPES> listOfFields =
                        postgres.listOfFields(
                            entity,
                            context.getProperty(DATASETID_PREFIX_TRUNCATE).getValue(),
                            context.getProperty(EXPORT_SYSATTRS).asBoolean(),
                            getIgnoredAttributes(context, flowFile)
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
                            ignoreEmptyObservedAt,
                            exportMode
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
                        if (replaceMode) {
                            String deleteSql = postgres.deleteEntityQuery(schemaName, tableName, entity.entityId);
                            getLogger().debug("Deleting existing rows for entity: {}", entity.entityId);
                            conn.createStatement().execute(deleteSql);
                        }
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
                addErrorAttributesToFlowFile(session, flowFile, e);
                result.routeTo(flowFile, REL_FAILURE);
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
        onFlowFileError = onFlowFileError.andThen((c, ff, r, e) -> {
            switch (r.destination()) {
                case Failure:
                    logErrorMessage("Failed to update database for {} due to {}; routing to failure", new Object[]{ff, e}, e);
                    addErrorAttributesToFlowFile(session, ff, e);
                    break;
                case Retry:
                    logErrorMessage(
                        "Failed to update database for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                        new Object[]{ff, e},
                        e
                    );
                    addErrorAttributesToFlowFile(session, ff, e);
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    private ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError =
            ExceptionHandler.createOnGroupError(context, session, result, REL_FAILURE, REL_RETRY);

        onGroupError = onGroupError.andThen((ctx, flowFileGroup, errorTypesResult, exception) -> {
            switch (errorTypesResult.destination()) {
                case Failure:
                    List<FlowFile> flowFilesToFailure = getFlowFilesOnRelationship(result, REL_FAILURE);
                    result.getRoutedFlowFiles().put(REL_FAILURE, addErrorAttributesToFlowFilesInGroup(session, flowFilesToFailure, flowFileGroup.getFlowFiles(), exception));
                    break;
                case Retry:
                    List<FlowFile> flowFilesToRetry = getFlowFilesOnRelationship(result, REL_RETRY);
                    result.getRoutedFlowFiles().put(REL_RETRY, addErrorAttributesToFlowFilesInGroup(session, flowFilesToRetry, flowFileGroup.getFlowFiles(), exception));
                    break;
            }
        });

        return onGroupError;
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
                        result.routeTo(addErrorAttributesToFlowFile(session, flowFile, e), REL_FAILURE);
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
                    result.routeTo(addErrorAttributesToFlowFile(session, failedFlowFile, e), REL_FAILURE);
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
            ExceptionHandler.OnError<RollbackOnFailure, FlowFileGroup> onGroupError = onGroupError(context, session, result);
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

    private List<FlowFile> getFlowFilesOnRelationship(RoutingResult result, final Relationship relationship) {
        return Optional.ofNullable(result.getRoutedFlowFiles().get(relationship))
            .orElse(emptyList());
    }

    private List<FlowFile> addErrorAttributesToFlowFilesInGroup(ProcessSession session, List<FlowFile> flowFilesOnRelationship, List<FlowFile> flowFilesInGroup, Exception exception) {
        return flowFilesOnRelationship.stream()
            .map(ff -> flowFilesInGroup.contains(ff) ? addErrorAttributesToFlowFile(session, ff, exception) : ff)
            .collect(toList());
    }

    private FlowFile addErrorAttributesToFlowFile(final ProcessSession session, FlowFile flowFile, final Exception exception) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ERROR_MESSAGE_ATTR, exception.getMessage());

        if (exception instanceof SQLException) {
            int errorCode = ((SQLException) exception).getErrorCode();
            String sqlState = ((SQLException) exception).getSQLState();

            if (errorCode > 0) {
                attributes.put(ERROR_CODE_ATTR, valueOf(errorCode));
            }

            if (sqlState != null) {
                attributes.put(ERROR_SQL_STATE_ATTR, sqlState);
            }
        }

        return session.putAllAttributes(flowFile, attributes);
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
            if (e instanceof SQLTransientException) {
                return ErrorTypes.TemporalFailure;
            } else if (e instanceof SQLException) {
                return ErrorTypes.InvalidInput;
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
            if (!(obj instanceof StatementFlowFileEnclosure other)) {
                return false;
            }

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
