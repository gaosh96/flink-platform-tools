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
        String jarFilePath = this.getClass().getClassLoader().getResource("myudf.jar").getPath();
        List<String> udfClassList = UdfClassSearcher.getUdfClassList(jarFilePath);
        System.out.println(udfClassList);
    }
}