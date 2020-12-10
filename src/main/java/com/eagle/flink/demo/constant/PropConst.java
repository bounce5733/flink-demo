package com.eagle.flink.demo.constant;

/**
 * 属性标识
 *
 * @author: yh.jiang
 * @time:
 */
public class PropConst {
    public static final String KAFKA_INDEX_SOURCE_TOPIC = "kafka.index.source.topic";
    public static final String KAFKA_INDEX_SINK_TOPIC = "kafka.index.sink.topic";

    public static final String FLINK_STREAM_PARALLELISM = "flink.stream.parallelism";
    public static final String FLINK_STREAM_CHECKPOINT_ENABLE = "flink.stream.checkpoint.enable";
    public static final String FLINK_STREAM_CHECKPOINT_INTERVAL = "flink.stream.checkpoint.interval";
    public static final String FLINK_FIXED_DELAY_RESTART_TIMES = "flink.fixed.delay.restart.times";
    public static final String FLINK_FIXED_DELAY_BETWEEN_ATTEMPTS = "flink.fixed.delay.between.attempts";
    public static final String FLINK_SINK_PARALLELISM = "flink.sink.parallelism";

    public static final String PROPERTIES_FILE_NAME = "/application.properties";

}
