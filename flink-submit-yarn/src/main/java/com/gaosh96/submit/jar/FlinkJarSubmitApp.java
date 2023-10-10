package com.gaosh96.submit.jar;

import com.gaosh96.submit.entity.FlinkJarJobConfig;
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
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.gaosh96.submit.constants.ApplicationConstants.FLINK_COMMON_LIBS;
import static com.gaosh96.submit.constants.ApplicationConstants.FLINK_CONF_NAME;
import static com.gaosh96.submit.constants.ApplicationConstants.FLINK_DIST_JAR;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/3
 */
public class FlinkJarSubmitApp {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJarSubmitApp.class);

    public static void submit(FlinkJarJobConfig jarConfig) {

        String flinkVersion = jarConfig.getFlinkVersion();
        String flinkVersionWithDots = flinkVersion.replaceAll("_", "\\.");

        // 可以放到配置中心，这里放到 resources 目录下面读取
        String flinkConfFilePath = FlinkJarSubmitApp.class.getClassLoader().getResource(String.format(FLINK_CONF_NAME, flinkVersion)).getPath();
        // 加载配置文件的 conf
        Configuration flinkConfig = ConfigurationUtils.loadYAMLResource(new File(flinkConfFilePath));

        // yarn-application 模式执行
        flinkConfig.set(DeploymentOptions.TARGET, "yarn-application");
        // 保存在 hdfs的公共的依赖 jar 包
        // -Dyarn.provided.lib.dirs
        List<String> libs = new ArrayList<>();
        libs.add(String.format(FLINK_COMMON_LIBS, flinkVersion));
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, libs);
        // flink dist jar，需要一个本地路径，例如 flink-dist_2.12-1.14.4.jar
        flinkConfig.set(YarnConfigOptions.FLINK_DIST_JAR, String.format(FLINK_DIST_JAR, flinkVersionWithDots, flinkVersionWithDots));
        // -d
        flinkConfig.set(DeploymentOptions.ATTACHED, true);
        // set job name
        flinkConfig.set(PipelineOptions.NAME, jarConfig.getJobName());
        // set job ID
        String jobId = new JobID().toHexString();
        flinkConfig.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId);
        // -yqu
        //flinkConfig.set(YarnConfigOptions.APPLICATION_QUEUE, jarConfig.getYarnQueue());
        // -ynm
        flinkConfig.set(YarnConfigOptions.APPLICATION_NAME, jarConfig.getJobName());
        // -yt 本地路径，上传到 hdfs 的临时目录
        flinkConfig.set(YarnConfigOptions.SHIP_FILES, jarConfig.getShipFiles());
        // -C  file:///xxx;file:///xxx
        flinkConfig.set(PipelineOptions.CLASSPATHS, jarConfig.getClasspathFiles());
        // -c
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, jarConfig.getMainClass());
        // jar
        flinkConfig.set(PipelineOptions.JARS, Collections.singletonList(jarConfig.getFlinkDistJar()));
        // 传入主类的 args
        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, jarConfig.getArgs());

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
