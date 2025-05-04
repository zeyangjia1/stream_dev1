package com.stream.common.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.stream.common.utils.FlinkEnvUtils
 * @Author zeyang_jia
 * @Date  2025/05/04
 * @description: Get Env
 */
public class FlinkEnvUtils {

    public static StreamExecutionEnvironment getFlinkRuntimeEnv(){
        if (CommonUtils.isIdeaEnv()){
            System.err.println("Action Local Env");
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
