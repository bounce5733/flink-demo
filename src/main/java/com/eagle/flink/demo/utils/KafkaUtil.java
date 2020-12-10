package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.applycount.IndexSchema;
import com.eagle.flink.demo.constant.PropConst;
import com.eagle.micro.model.flink.IndexEvent;
import com.eagle.micro.model.flink.IndexSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaUtil {

    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    private static final String KAFKA_OFFSET_RESET_KEY = "kafka_offset_reset"; // 消费位移
    private static final String KAFKA_OFFSET_RESET_DEFAULT = "latest";

    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get("kafka.brokers"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.JsonDeserializer");
        props.put("auto.offset.reset", parameterTool.get(KAFKA_OFFSET_RESET_KEY, KAFKA_OFFSET_RESET_DEFAULT));
        return props;
    }

    /**
     * 构建kafka数据流
     *
     * @param env
     * @param groupId
     * @return
     * @throws IllegalAccessException
     */
    public static DataStreamSource<IndexSource> buildSource(StreamExecutionEnvironment env, String groupId) throws IllegalAccessException {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer<IndexSource> consumer = new FlinkKafkaConsumer<>(
                parameterTool.get(PropConst.KAFKA_INDEX_SOURCE_TOPIC),
                new IndexSchema(),
                props);
        return env.addSource(consumer);
    }

    public static FlinkKafkaProducer<IndexEvent> buildSink(ParameterTool parameterTool) {
        return new FlinkKafkaProducer<>(parameterTool.get("kafka.brokers"),
                parameterTool.get(PropConst.KAFKA_INDEX_SINK_TOPIC),
                new IndexSchema());
    }
}
