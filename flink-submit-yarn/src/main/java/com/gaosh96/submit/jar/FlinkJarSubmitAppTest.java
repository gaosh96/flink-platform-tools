package com.gaosh96.submit.jar;

import com.gaosh96.submit.utils.ConfigurationUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/3
 */
public class FlinkJarSubmitAppTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJarSubmitAppTest.class);

    /**
     * submit job with yarn-application mode
     *
     * @param jarConfig
     * @return
     */
    public static void main(String[] args) {
        // 可以放到配置中心，这里放到 resources 目录下面读取
        String flinkConfFilePath = "/Users/gaosh/apps/flink-1.14.4/conf/flink-conf.yaml";
        // 加载配置文件的 conf
        Configuration flinkConfig = ConfigurationUtils.loadYAMLResource(new File(flinkConfFilePath));

        // yarn-application 模式执行
        flinkConfig.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        // 保存在 hdfs的公共的依赖 jar 包
        // -Dyarn.provided.lib.dirs
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList("hdfs://hadoop001:8020/flink_1_14_4/libs"));
        // flink dist jar，需要一个本地路径，例如 flink-dist_2.12-1.14.4.jar
        flinkConfig.set(YarnConfigOptions.FLINK_DIST_JAR, "/Users/gaosh/apps/flink-1.14.4/lib/flink-dist_2.12-1.14.4.jar");
        // -d
        flinkConfig.set(DeploymentOptions.ATTACHED, true);
        // -p
        flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        // set job name
        flinkConfig.set(PipelineOptions.NAME, "SocketWindowWordCount");
        // set job ID
        String jobId = new JobID().toHexString();
        flinkConfig.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
        // -ynm
        flinkConfig.set(YarnConfigOptions.APPLICATION_NAME, "SocketWindowWordCount");
        // -c
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, "org.apache.flink.streaming.examples.socket.SocketWindowWordCount");
        // jar
        flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("/Users/gaosh/apps/flink-1.14.4/examples/streaming/SocketWindowWordCount.jar"));
        // 传入主类的 args
        List<String> argList = new ArrayList<>();
        argList.add("--hostname");
        argList.add("localhost");
        argList.add("--port");
        argList.add("9999");
        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, argList);

        // 手动指定 log4j 日志位置，fix: The file LOG does not exist on the TaskExecutor.
        flinkConfig.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, "/Users/gaosh/apps/flink-1.14.4/conf/log4j.properties");

        // deploy job
        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
        ClusterClientFactory<ApplicationId> clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);
        ClusterDescriptor<ApplicationId> clusterDescriptor = clusterClientFactory.createClusterDescriptor(flinkConfig);

        ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(flinkConfig);
        ApplicationConfiguration applicationConfiguration = ApplicationConfiguration.fromConfiguration(flinkConfig);
        ClusterClient<ApplicationId> clusterClient = null;

        try {
            clusterClient = clusterDescriptor.deployApplicationCluster(clusterSpecification, applicationConfiguration).getClusterClient();
            String applicationId = clusterClient.getClusterId().toString();
            String jobManagerUrl = clusterClient.getWebInterfaceURL();
            LOG.info("applicationId: {}", applicationId);
            LOG.info("jobManagerUrl: {}", jobManagerUrl);
        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
        } finally {
            if (clusterClient != null) {
                clusterClient.close();
            }
            if (clusterDescriptor != null) {
                clusterDescriptor.close();
            }
        }

    }

}
