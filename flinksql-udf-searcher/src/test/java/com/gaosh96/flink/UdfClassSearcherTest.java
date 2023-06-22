package com.gaosh96.flink;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author gaosh
 * @version 1.0
 * @date 2023/6/20
 */
class UdfClassSearcherTest {

    @Test
    void getUdfClassList() throws IOException {

        List<String> udfClassList = UdfClassSearcher.getUdfClassList("");
        System.out.println(udfClassList);

    }
}