package com.gaosh96.submit.jar;

import com.gaosh96.submit.entity.FlinkJarJobConfig;
import com.gaosh96.submit.entity.SubmitResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/11
 */
public class FlinkJarSubmitAppTest {

    /**
     * 配置环境变量 HADOOP_HOME=/Users/gaosh/apps/hadoop
     */
    @Test
    public void testJarSubmit() {

        String jarFilePath = "/Users/gaosh/apps/flink-1.14.4/examples/streaming/SocketWindowWordCount.jar";
        String flinkVersion = "1_14_4";
        String jobName = "SocketWindowWordCount";
        String mainClass = "org.apache.flink.streaming.examples.socket.SocketWindowWordCount";
        //String clusterName = "hadoop-cluster01";
        List<String> args = new ArrayList<>();
        args.add("--hostname");
        args.add("localhost");
        args.add("--port");
        args.add("9999");

        FlinkJarJobConfig jarJobConfig = FlinkJarJobConfig.builder()
                //.clusterName(clusterName)
                .flinkVersion(flinkVersion)
                .jarFilePath(jarFilePath)
                .jobName(jobName)
                .slotPerTm(1)
                .parallelism(2)
                .tmMemorySize("2048")
                .jmMemorySize("1024")
                .mainClass(mainClass)
                .args(args)
                .build();

        SubmitResponse response = FlinkJarSubmitApp.submit(jarJobConfig);
        System.out.println(response);

    }


}
