package com.gaosh96.flink.sql.utils;

import com.gaosh96.flink.utils.StatementUtils;
import org.junit.Test;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/15
 */
public class StatementUtilsTest {

    @Test
    public void testZipStatement() {
        String init = "CREATE CATALOG batch_hive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'default-database' = 'default',\n" +
                "    'hive-conf-dir' = '/Users/gaosh/apps/hive/conf',\n" +
                "    'hive-version' = '3.1.2'\n" +
                ");";
        String stmt = "create view t as\n" +
                "select 1 as user_id, 1 as item_id, 'play' as behavior union all\n" +
                "select 2 as user_id, 2 as item_id, 'soccer' as behavior;\n" +
                "insert into batch_hive.ods.kafka_test\n" +
                "select \n" +
                "user_id\n" +
                "as item_id\n" +
                "behavior\n" +
                "from t;\n";

        String zip = StatementUtils.zipStatement(stmt);
        System.out.println(zip);
    }

    @Test
    public void testUnzipStatement() {
        String zipedStmt = "H4sIAAAAAAAAAFWMQQ6DMAwE736Fb1wqpHLlMZEJRrEIcZWYVP19oS2qcttd7YzPTMZYhZ9oSAUKR/aG9yPjXjg7mW/fJsbbp3WPSK/unCYOVEUz7kk0IcV48UPDDy1f1HvOjWEEScfbUJIpTmQ+uCCVe51Lv9KykjMudunhp4a/Fy4VLFk3tBHeaPpICNwAAAA=";
        String unzip = StatementUtils.unzipStatement(zipedStmt);
        System.out.println(unzip);
    }
}
