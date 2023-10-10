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

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.client.gateway.context.DefaultContext;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import static com.gaosh96.flink.utils.YarnConfigOptions.APPLICATION_ID;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.configuration.PipelineOptions.OPERATOR_CHAINING;
import static org.apache.flink.table.client.config.ResultMode.TABLEAU;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

public class LocalContextUtils {

    public static DefaultContext buildDefaultContext(String sessionId) {
        // load config
        String flinkConfigDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        Configuration configuration = GlobalConfiguration.loadConfiguration(flinkConfigDir);
        // job name
        configuration.set(NAME, "flinksql query test");
        // yarn application id
        configuration.set(APPLICATION_ID, sessionId);
        configuration.set(EXECUTION_RESULT_MODE, TABLEAU);
        configuration.set(OPERATOR_CHAINING, false);

        List<URL> dependencies = Collections.emptyList();
        List<CustomCommandLine> commandLines =
                CliFrontend.loadCustomCommandLines(configuration, flinkConfigDir);

        return new DefaultContext(dependencies, configuration, commandLines);
    }

}
