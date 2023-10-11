package com.gaosh96.submit.entity;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.List;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/10
 */
@Data
@SuperBuilder
public class JobConfig {

    // -ynm & -Dpipeline.name
    private String jobName;
    // -yqu
    private String yarnQueue;
    // -yjm
    private String jmMemorySize;
    // -ytm
    private String tmMemorySize;
    // -ys
    private String slotPerTm;
    // -p
    private int parallelism;

    private String jarFilePath;

    // 1_14_4
    // 1_12_2
    private String flinkVersion;

    private List<String> shipFiles;

    private List<String> classpathFiles;

    private String flinkDistJar;

    // hadoop cluster name
    private String clusterName;

}
