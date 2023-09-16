package com.gaosh96.flink.utils;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogOperation;
import org.apache.flink.table.utils.PrintUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.table.api.internal.TableResultImpl.TABLE_RESULT_OK;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;

/**
 * @author gaosh96
 * @version 1.0
 * @since 2023/9/11
 */
public class OperationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(OperationExecutor.class);

    private Executor executor;
    private String sessionId;

    private static final int MAX_ROWS = 1000;
    private static final int MAX_DURATION_SECONDS = 120;

    public OperationExecutor(Executor executor, String sessionId) {
        this.executor = executor;
        this.sessionId = sessionId;
    }

    public void callOperation(Operation operation) {

        if ((operation instanceof SetOperation) ||
                (operation instanceof ResetOperation) ||
                (operation instanceof CatalogSinkModifyOperation) ||
                (operation instanceof BeginStatementSetOperation) ||
                (operation instanceof EndStatementSetOperation)) {
            throw new SqlExecutionException("only support select operation");
        }

        if (operation instanceof QueryOperation) {
            callSelect((QueryOperation) operation);
        } else if (operation instanceof CreateCatalogOperation) {
            // create catalog
            executeOperation(operation);
        } else {
            throw new SqlExecutionException("only support select operation");
        }
    }

    private void callSelect(QueryOperation operation) {
        ResultDescriptor resultDesc = executor.executeQuery(sessionId, operation);
        String resultId = resultDesc.getResultId();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                executor.cancelQuery(sessionId, resultId);
                Thread.sleep(5000L);
            } catch (Exception e) {
                // ignore
            }
        }));

        List<Column> columns = resultDesc.getResultSchema().getColumns();
        final String[] fieldNames =
                Stream.concat(
                        Stream.of(PrintUtils.ROW_KIND_COLUMN),
                        columns.stream().map(Column::getName))
                        .toArray(String[]::new);

        LOG.info(String.join("\t", fieldNames));

        final AtomicInteger receivedRowCount = new AtomicInteger(0);
        long startTs = new Date().getTime() / 1000;
        boolean isRunning = true;

        while (isRunning) {

            if (isTerminalTime(startTs)) {
                try {
                    executor.cancelQuery(sessionId, resultId);
                } catch (SqlExecutionException e) {
                    // ignore
                }
                LOG.info("reach max query duration, cancel job");
                return;
            }

            final TypedResult<List<Row>> result = executor.retrieveResultChanges(sessionId, resultId);

            switch (result.getType()) {
                case EMPTY:
                    try {
                        // wait 100 ms
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        isRunning = false;
                    }

                    break;
                case EOS:
                    try {
                        executor.cancelQuery(sessionId, resultId);
                    } catch (SqlExecutionException e) {
                        // ignore
                    }

                    isRunning = false;
                    break;
                case PAYLOAD:
                    List<Row> changes = result.getPayload();
                    for (Row change : changes) {
                        int total = receivedRowCount.incrementAndGet();

                        if (total > MAX_ROWS || isTerminalTime(startTs)) {
                            try {
                                executor.cancelQuery(sessionId, resultId);
                            } catch (SqlExecutionException e) {
                                // ignore
                            }

                            isRunning = false;
                        }

                        final String[] row =
                                PrintUtils.rowToString(
                                        change,
                                        PrintUtils.NULL_COLUMN,
                                        true,
                                        resultDesc.getResultSchema(),
                                        ZoneId.of("Asia/Shanghai"));

                        LOG.info(String.join("\t", row));
                    }

                    break;
                default:
                    LOG.info("resultType: {}", result.getType());
                    LOG.info("fetch query result error, please retry");
                    isRunning = false;
            }
        }

    }


    private void executeOperation(Operation operation) {
        TableResult result = executor.executeOperation(sessionId, operation);
        if (TABLE_RESULT_OK == result) {
            LOG.info(MESSAGE_EXECUTE_STATEMENT);
        } else {
            LOG.info("Execute error: {}", operation.asSummaryString());
        }
    }

    private boolean isTerminalTime(long startTs) {
        long nowTs = new Date().getTime() / 1000;
        long duration = nowTs - startTs;

        return duration > MAX_DURATION_SECONDS;
    }

}
