package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.constant.PropConst;
import com.eagle.flink.demo.model.IndexEvent;
import com.eagle.flink.demo.model.IndexSource;
import com.eagle.flink.demo.schema.IndexSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaUtil {

    public static Properties buildKafkaProps() {
        return buildKafkaProps(ParameterTool.fromSystemProperties());
    }

    public static Properties buildKafkaProps(ParameterTool parameterTool) {
        Properties props = parameterTool.getProperties();
        props.put("bootstrap.servers", parameterTool.get("kafka.brokers"));
        props.put("zookeeper.connect", parameterTool.get("kafka.zookeeper.connect"));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.JsonDeserializer");
        props.put("auto.offset.reset", "latest");
        return props;
    }

    public static DataStreamSource<IndexSource> buildSource(StreamExecutionEnvironment env, String groupId) throws IllegalAccessException {
        ParameterTool parameter = (ParameterTool) env.getConfig().getGlobalJobParameters();
        String topic = parameter.getRequired(PropConst.KAFKA_INDEX_SOURCE_TOPIC);
        Long time = parameter.getLong(PropConst.CONSUMER_FROM_TIME, 0L);
        return buildSource(env, topic, time, groupId);
    }

    /**
     * @param env
     * @param topic
     * @param time  订阅的时间
     * @return
     * @throws IllegalAccessException
     */
    public static DataStreamSource<IndexSource> buildSource(StreamExecutionEnvironment env, String topic, Long time, String groupId) throws IllegalAccessException {
        ParameterTool parameterTool = (ParameterTool) env.getConfig().getGlobalJobParameters();
        Properties props = buildKafkaProps(parameterTool);
        props.setProperty("group.id", groupId);
        FlinkKafkaConsumer<IndexSource> consumer = new FlinkKafkaConsumer<>(
                topic,
                new IndexSchema(),
                props);
        //重置offset到time时刻
        if (time != 0L) {
            Map<KafkaTopicPartition, Long> partitionOffset = buildOffsetByTime(props, parameterTool, time);
            consumer.setStartFromSpecificOffsets(partitionOffset);
        }
        return env.addSource(consumer);
    }

    private static Map<KafkaTopicPartition, Long> buildOffsetByTime(Properties props, ParameterTool parameterTool, Long time) {
        props.setProperty("group.id", "query_time_" + time);
        KafkaConsumer consumer = new KafkaConsumer(props);
        List<PartitionInfo> partitionsFor = consumer.partitionsFor(parameterTool.getRequired(PropConst.KAFKA_INDEX_SOURCE_TOPIC));
        Map<TopicPartition, Long> partitionInfoLongMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionsFor) {
            partitionInfoLongMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), time);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsetResult = consumer.offsetsForTimes(partitionInfoLongMap);
        Map<KafkaTopicPartition, Long> partitionOffset = new HashMap<>();
        offsetResult.forEach((key, value) -> partitionOffset.put(new KafkaTopicPartition(key.topic(), key.partition()), value.offset()));

        consumer.close();
        return partitionOffset;
    }

    public static FlinkKafkaProducer<IndexEvent> buildSink(ParameterTool parameterTool) {
        return new FlinkKafkaProducer<>(parameterTool.get("kafka.brokers"),
                parameterTool.get(PropConst.KAFKA_INDEX_SINK_TOPIC),
                new IndexSchema());
    }
}
