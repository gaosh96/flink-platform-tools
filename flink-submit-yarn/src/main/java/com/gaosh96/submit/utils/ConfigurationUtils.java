package com.gaosh96.submit.utils;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/3
 */
public class ConfigurationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

    //private static final String HADOOP_CONF_FILE_NAMES = "core-site.xml,hdfs-site.xml,yarn-site.xml";

    public static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    config.setString(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    //public static void mergeHadoopAndFlinkConfiguration(String clusterName, Configuration flinkConfig) {
    //    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();
    //    for (String confFile : HADOOP_CONF_FILE_NAMES.split(",")) {
    //        hadoopConfig.addResource(ConfigurationUtils.class.getClassLoader().getResource(clusterName + "/" + confFile));
    //    }
    //
    //    hadoopConfig.iterator().forEachRemaining(x -> flinkConfig.setString(x.getKey(), x.getValue()));
    //}

}
