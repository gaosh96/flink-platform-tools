/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gaosh96.flink.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 解析输入的 sql，去除注释、末尾分号
 * 拆分多条 sql
 */
public class StatementNormalizeUtils {

    private static final String MASK = "--.*$";
    private static final String BEGINNING_MASK = "^(\\s)*--.*$";

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
