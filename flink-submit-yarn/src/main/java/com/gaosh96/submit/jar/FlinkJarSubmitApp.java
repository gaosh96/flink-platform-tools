package com.gaosh96.submit.jar;

import com.gaosh96.submit.entity.FlinkJarJobConfig;
import com.gaosh96.submit.entity.SubmitResponse;
import com.gaosh96.submit.utils.ClusterUtils;
import com.gaosh96.submit.utils.ConfigurationUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private static final ExecutorService executorService =
            new ThreadPoolExecutor(
                    Runtime.getRuntime().availableProcessors() * 5,
                    Runtime.getRuntime().availableProcessors() * 10,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1024),
                    new ThreadFactoryBuilder().setNameFormat("flink-executor" + "-%d").setDaemon(true).build(),
                    new ThreadPoolExecutor.AbortPolicy());

    /**
     * submit job with yarn-application mode
     *
     * @param jarConfig
     * @return
     */
    public static SubmitResponse submit(FlinkJarJobConfig jarConfig) {

        String flinkVersion = jarConfig.getFlinkVersion();
        String flinkVersionWithDots = flinkVersion.replaceAll("_", "\\.");
        //String clusterName = jarConfig.getClusterName();

        // 可以放到配置中心，这里放到 resources 目录下面读取
        String flinkConfFilePath = FlinkJarSubmitApp.class.getClassLoader().getResource(String.format(FLINK_CONF_NAME, flinkVersion)).getPath();
        // 加载配置文件的 conf
        Configuration flinkConfig = ConfigurationUtils.loadYAMLResource(new File(flinkConfFilePath));

        // yarn-application 模式执行
        flinkConfig.set(DeploymentOptions.TARGET, "yarn-application");
        // 保存在 hdfs的公共的依赖 jar 包
        // -Dyarn.provided.lib.dirs
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(String.format(FLINK_COMMON_LIBS, flinkVersion)));
        // flink dist jar，需要一个本地路径，例如 flink-dist_2.12-1.14.4.jar
        flinkConfig.set(YarnConfigOptions.FLINK_DIST_JAR, String.format(FLINK_DIST_JAR, flinkVersionWithDots, flinkVersionWithDots));
        // -d
        flinkConfig.set(DeploymentOptions.ATTACHED, true);
        // -p
        flinkConfig.set(CoreOptions.DEFAULT_PARALLELISM, jarConfig.getParallelism());
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
        //flinkConfig.set(YarnConfigOptions.SHIP_FILES, jarConfig.getShipFiles());
        // -C  file:///xxx;file:///xxx
        //flinkConfig.set(PipelineOptions.CLASSPATHS, jarConfig.getClasspathFiles());
        // -c
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, jarConfig.getMainClass());
        // jar
        flinkConfig.set(PipelineOptions.JARS, Collections.singletonList(jarConfig.getJarFilePath()));
        // 传入主类的 args
        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, jarConfig.getArgs());

        // 手动指定 log4j 日志位置，fix: The file LOG does not exist on the TaskExecutor.
        flinkConfig.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, "/Users/gaosh/apps/flink-1.14.4/conf/log4j.properties");

        // 指定作业资源
        // -ys
        flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, jarConfig.getSlotPerTm());
        // -ytm
        flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(jarConfig.getTmMemorySize(), MemorySize.MemoryUnit.MEGA_BYTES));
        // -yjm
        flinkConfig.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(jarConfig.getJmMemorySize(), MemorySize.MemoryUnit.MEGA_BYTES));

        // savepoint config
        //flinkConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, "");
        //flinkConfig.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, false);

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

            return SubmitResponse.builder()
                    .jobId(jobId)
                    .applicationId(applicationId)
                    .jobManagerUrl(jobManagerUrl)
                    .status("success").build();

        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (clusterClient != null) {
                clusterClient.close();
            }
            if (clusterDescriptor != null) {
                clusterDescriptor.close();
            }
        }

    }

    /**
     * cancel job
     *
     * @param applicationId
     * @param jobId
     */
    public static void cancelJob(String applicationId, String jobId) {
        ClusterClient<ApplicationId> clusterClient = ClusterUtils.getClusterClient(applicationId);
        if (clusterClient == null) {
            LOG.error("clusterClient is null");
            return;
        }
        JobID jobID = JobID.fromHexString(jobId);
        CompletableFuture<Acknowledge> cancelFuture = clusterClient.cancel(jobID);
        cancelFuture.whenCompleteAsync((acknowledge, throwable) -> {
            if (throwable != null) {
                LOG.error(throwable.getMessage());
            } else {
                LOG.info("application: {}, jobId: {} has been cancelled", applicationId, jobId);
            }
        }, executorService);
    }

    /**
     * cancel job
     *
     * @param applicationId
     * @param jobId
     */
    public static void cancelJobWithSavepoint(String applicationId, String jobId, String savepointPath) {
        ClusterClient<ApplicationId> clusterClient = ClusterUtils.getClusterClient(applicationId);
        if (clusterClient == null) {
            LOG.error("clusterClient is null");
            return;
        }

        JobID jobID = JobID.fromHexString(jobId);
        CompletableFuture<String> cancelWithSavepointFuture
                = clusterClient.cancelWithSavepoint(jobID, savepointPath);
        cancelWithSavepointFuture.whenCompleteAsync((located, throwable) -> {
            if (throwable != null) {
                LOG.error(throwable.getMessage());
            } else {
                LOG.info("application: {}, jobId: {} has been cancelled with savepoint, savepoint path is {}", applicationId, jobId, located);
            }
        }, executorService);
    }


    /**
     * trigger savepoint
     *
     * @param applicationId
     * @param jobId
     * @param savepointPath
     */
    public static void triggerSavepoint(String applicationId, String jobId, String savepointPath) {
        ClusterClient<ApplicationId> clusterClient = ClusterUtils.getClusterClient(applicationId);
        if (clusterClient == null) {
            LOG.error("clusterClient is null");
            return;
        }

        JobID jobID = JobID.fromHexString(jobId);
        CompletableFuture<String> savepointFuture = clusterClient.triggerSavepoint(jobID, savepointPath);
        savepointFuture.whenCompleteAsync((located, throwable) -> {
            if (throwable != null) {
                LOG.error(throwable.getMessage());
            } else {
                LOG.info("application: {}, jobId: {} trigger savepoint success, savepoint path is {}", applicationId, jobId, located);
            }
        }, executorService);
    }
}
