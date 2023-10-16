package com.gaosh96.flink.utils;

import org.apache.commons.io.IOUtils;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.gaosh96.flink.utils.StatementConstants.INSERT_INTO_SIGN;
import static com.gaosh96.flink.utils.StatementConstants.SET_SIGN;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/15
 */
public class StatementUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StatementUtils.class);

    private static final String MASK = "--.*$";
    private static final String BEGINNING_MASK = "^(\\s)*--.*$";

    public static StatementSet parseSql(StreamTableEnvironment tableEnv, List<String> sqlLines) {

        StreamStatementSet stmt = tableEnv.createStatementSet();

        sqlLines.forEach(s -> {
            Arrays.stream(StatementType.values()).forEach(t -> {
                Matcher matcher = t.getPattern().matcher(s);
                boolean matches = matcher.matches();
                if (matches) {
                    if (t.name().equals(SET_SIGN)) {
                        // set k = v
                        String key = matcher.group(2).trim();
                        String value = matcher.group(3).trim();
                        tableEnv.getConfig().getConfiguration().setString(key, value);
                        LOG.info("set param: {} = {}", key, value);
                    } else if (t.name().equals(INSERT_INTO_SIGN)) {
                        // insert into
                        stmt.addInsertSql(s);
                    } else {
                        // create table / function / catalog ... etc
                        tableEnv.executeSql(s);
                    }

                }
            });
        });

        return stmt;
    }

    public static String zipStatement(@Nonnull String stmt) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gos = null;
        try {
            gos = new GZIPOutputStream(bos);
            gos.write(stmt.getBytes(StandardCharsets.UTF_8));
            gos.close();
            return Base64.getEncoder().encodeToString(bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(bos, gos);
        }

        return null;
    }

    public static String unzipStatement(@Nonnull String zipedStmt) {
        byte[] decode = Base64.getDecoder().decode(zipedStmt);
        ByteArrayInputStream bis = new ByteArrayInputStream(decode);
        GZIPInputStream gis = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try {
            gis = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int size;
            while ((size = gis.read(buffer)) != -1) {
                bos.write(buffer, 0, size);
            }
            return bos.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(bis, gis, bos);
        }

        return null;
    }

    /**
     * @param content
     * @return
     */
    public static List<String> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        List<String> buffer = new ArrayList<>();

        for (String line : content.split("\n")) {
            if (isEndOfStatement(line)) {
                buffer.add(line);
                statements.add(normalize(buffer));
                buffer.clear();
            } else {
                buffer.add(line);
            }
        }

        if (!buffer.isEmpty()) {
            statements.add(normalize(buffer));
        }
        return statements;
    }

    private static String normalize(List<String> buffer) {
        // remove comment lines
        String stmt = buffer.stream()
                .map(statementLine -> statementLine.replaceAll(BEGINNING_MASK, ""))
                .collect(Collectors.joining("\n"));

        // remove ending semicolon
        if (stmt.endsWith(";")) {
            return stmt.substring(0, stmt.length() - 1);
        }

        return stmt;
    }

    private static boolean isEndOfStatement(String line) {
        return line.replaceAll(MASK, "").trim().endsWith(";");
    }


}
