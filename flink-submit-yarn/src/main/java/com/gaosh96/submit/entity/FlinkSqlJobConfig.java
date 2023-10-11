package com.gaosh96.submit.entity;

import lombok.Data;
import lombok.experimental.SuperBuilder;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/10
 */
@Data
@SuperBuilder
public class FlinkSqlJobConfig extends JobConfig {

    private String sql;
    private String extJarFiles;

}
