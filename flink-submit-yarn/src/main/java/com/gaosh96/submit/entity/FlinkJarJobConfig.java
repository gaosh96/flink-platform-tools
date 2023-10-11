package com.gaosh96.submit.entity;

import lombok.Builder;
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
public class FlinkJarJobConfig extends JobConfig {

    private String mainClass;
    private List<String> args;

}
