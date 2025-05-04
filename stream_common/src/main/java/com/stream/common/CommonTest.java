package com.stream.common;

import com.stream.common.utils.ConfigUtils;

/**
 * @Package com.stream.common.CommonTest
 * @Author zeyang_jia
 * @Date 2025/05/04
 * @description: Test
 */
public class CommonTest {

    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }


}
