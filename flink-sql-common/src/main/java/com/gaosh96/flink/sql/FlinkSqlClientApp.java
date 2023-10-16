package com.gaosh96.flink.sql;

import com.gaosh96.flink.utils.StatementUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.gaosh96.flink.utils.StatementConstants.INIT_SIGN;
import static com.gaosh96.flink.utils.StatementConstants.SQL_SIGN;

/**
 * 用于解析传参传入的 sql 和其他信息，以 jar 的形式打包提交
 *
 * @author gaosh
 * @version 1.0
 * @since 2023/10/15
 */
public class FlinkSqlClientApp {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlClientApp.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ParameterTool params = ParameterTool.fromArgs(args);

        // init sql, such as common udf, catalog, etc..
        // gzip + base64
        String init = StatementUtils.unzipStatement(params.get(INIT_SIGN));
        // exec sql
        // gzip + base64
        String sql = StatementUtils.unzipStatement(params.get(SQL_SIGN));

        List<String> initSqlLines = StatementUtils.splitContent(init);
        List<String> execSqlLines = StatementUtils.splitContent(sql);
        initSqlLines.addAll(execSqlLines);

        LOG.info("stmt: {}", initSqlLines);

        StatementSet stmt = StatementUtils.parseSql(tableEnv, initSqlLines);

        stmt.execute();
    }

}
