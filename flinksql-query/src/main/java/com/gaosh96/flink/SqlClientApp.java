package com.gaosh96.flink;

import com.gaosh96.flink.utils.LocalContextUtils;
import com.gaosh96.flink.utils.OperationExecutor;
import com.gaosh96.flink.utils.StatementNormalizeUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author gaosh96
 * @version 1.0
 * @since 2023/9/9
 */
public class SqlClientApp {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClientApp.class);

    public static void main(String[] args) {
        String sessionId = args[0];

        LOG.info("sessionId: {}", sessionId);

        // init catalog
        String initCatalogFilePath = args[1];

        LOG.info("initCatalogFilePath: {}", initCatalogFilePath);

        List<String> initStmtsList;
        try {
            String initStmtsLine = FileUtils.readFileToString(new File(initCatalogFilePath), StandardCharsets.UTF_8).trim();
            initStmtsList = StatementNormalizeUtils.splitContent(initStmtsLine);
        } catch (IOException e) {
            LOG.error("file now found: {}", initCatalogFilePath);
            return;
        }

        // sql stmt
        String selectStmt = args[2];
        LOG.info("selectStmt: {}", selectStmt);

        selectStmt = StatementNormalizeUtils.splitContent(selectStmt).get(0);

        DefaultContext defaultContext = LocalContextUtils.buildDefaultContext(sessionId);
        LocalExecutor localExecutor = new LocalExecutor(defaultContext);
        localExecutor.openSession(sessionId);
        OperationExecutor operationExecutor = new OperationExecutor(localExecutor, sessionId);

        /**
         * exec init stmts
         */
        for (String stmt : initStmtsList) {
            Operation operation = localExecutor.parseStatement(sessionId, stmt);
            LOG.info("operation: {}", operation);
            operationExecutor.callOperation(operation);
        }

        /**
         * exec select stmt
         */
        Operation operation = localExecutor.parseStatement(sessionId, selectStmt);
        operationExecutor.callOperation(operation);

    }
}
