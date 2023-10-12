package com.gaosh96.submit.utils;

import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/12
 */
public class ClusterUtils {

    public static ClusterClient<ApplicationId> getClusterClient(String applicationId) {

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(YarnConfigOptions.APPLICATION_ID, applicationId);
        YarnClusterClientFactory clusterClientFactory = new YarnClusterClientFactory();
        ApplicationId appId = clusterClientFactory.getClusterId(flinkConfig);
        if (appId == null) {
            return null;
        }

        YarnClusterDescriptor clusterDescriptor = clusterClientFactory.createClusterDescriptor(flinkConfig);
        ClusterClient<ApplicationId> clusterClient = null;
        try {
            clusterClient = clusterDescriptor.retrieve(appId).getClusterClient();
        } catch (ClusterRetrieveException e) {
            e.printStackTrace();
        }

        return clusterClient;
    }

}
