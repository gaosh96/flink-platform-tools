package com.gaosh96.flink.utils;

import lombok.Getter;

import java.util.regex.Pattern;

/**
 * @author gaosh
 * @version 1.0
 * @since 2023/10/16
 */
@Getter
public enum StatementType {

    SET("SET(\\s+(\\S+)\\s*=(.*))?"),

    CREATE_TABLE("(CREATE\\s+TABLE.*)"),

    CREATE_FUNCTION("(CREATE\\s+FUNCTION.*)"),

    CREATE_VIEW("(CREATE\\s+VIEW.*)"),

    CREATE_CATALOG("(CREATE\\s+CATALOG.*)"),

    // flinksql 对于 with as 后跟 insert 暂未支持，仅支持 with as 后面跟 select
    //WITH_AS("(WITH\\s+(.*)\\s+AS\\s+\\(.*?\\)(,\\s+(.*)\\s+AS\\s+\\(.*?\\))*\\s*)INSERT"),

    INSERT_INTO("(INSERT\\s+INTO.*)");

    public final Pattern pattern;

    StatementType(String regex) {
        this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    }

}
