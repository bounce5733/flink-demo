package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.constant.PropConst;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class ExecutionEnvUtil {
    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(com.eagle.flink.demo.utils.ExecutionEnvUtil.class.getResourceAsStream(PropConst.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(null == args ? new String[]{} : args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(com.eagle.flink.demo.utils.ExecutionEnvUtil.class.getResourceAsStream(PropConst.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(PropConst.FLINK_STREAM_PARALLELISM));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(parameterTool.getInt(PropConst.FLINK_FIXED_DELAY_RESTART_TIMES),
                parameterTool.getInt(PropConst.FLINK_FIXED_DELAY_BETWEEN_ATTEMPTS)));
        if (parameterTool.getBoolean(PropConst.FLINK_STREAM_CHECKPOINT_ENABLE)) {
            env.enableCheckpointing(parameterTool.getLong(PropConst.FLINK_STREAM_CHECKPOINT_INTERVAL));
        }
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }
}