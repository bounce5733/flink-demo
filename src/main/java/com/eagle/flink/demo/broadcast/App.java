package com.eagle.flink.demo.broadcast;

import com.eagle.flink.demo.utils.ExecutionEnvUtil;
import com.eagle.flink.demo.utils.KafkaUtil;
import com.eagle.micro.model.flink.IndexEvent;
import com.eagle.micro.model.flink.IndexSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.HeapBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 广播流：<br>
 * 广播流可以通过查询配置文件，广播到某个 operator 的所有并发实例中，然后与另一条流数据连接进行计算。
 *
 * @author: yh.jiang
 * @time: 2020/12/22 14:19
 */
@Slf4j
public class App {

    private static final String CONFIG_KEY = "threshold";

    public static void main(String[] args) throws Exception {

        final MapStateDescriptor<String, String> CONFIG_DESCRIPTOR = new MapStateDescriptor<>(
                "ConfigBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<IndexSource> sourceData = KafkaUtil.buildSource(env, "APPLY_COUNT");
        sourceData.name("接收业务源数据");
        // 注册成配置广播数据源
        BroadcastStream<String> configDataSource = env.addSource(new MinuteBroadcastSource()).setParallelism(1).broadcast(CONFIG_DESCRIPTOR);
        // 连接广播流和处理数据流
        SingleOutputStreamOperator<IndexSource> filterDataSource = sourceData.connect(configDataSource).process(new BroadcastProcessFunction<IndexSource, String, IndexSource>() {
            @Override
            public void processElement(IndexSource indexSource, ReadOnlyContext readOnlyContext, Collector<IndexSource> collector) throws Exception {
                HeapBroadcastState config = (HeapBroadcastState) readOnlyContext.getBroadcastState(CONFIG_DESCRIPTOR);
                String configVal = String.valueOf(config.get(CONFIG_KEY));
                log.info("处理配置：{}", configVal);
                log.info("处理业务数据：{}", new ObjectMapper().writeValueAsString(indexSource));
                if (configVal.indexOf(indexSource.getAreaCode()) > 0) {
                    collector.collect(indexSource);
                }
            }

            @Override
            public void processBroadcastElement(String s, Context context, Collector<IndexSource> collector) throws Exception {
                log.info("收到配置:" + s);
                BroadcastState<String, String> state = context.getBroadcastState(CONFIG_DESCRIPTOR);
                context.getBroadcastState(CONFIG_DESCRIPTOR).clear();
                context.getBroadcastState(CONFIG_DESCRIPTOR).put(CONFIG_KEY, s);
            }
        });
        SingleOutputStreamOperator<ComputeModel> computerData = filterDataSource
                .keyBy(new MultiKey())
                .window(new TodayWindow())
                .trigger(new EveryoneTrigger())
                .aggregate(new Compute())
                .name("分组统计进件量");
        SingleOutputStreamOperator<IndexEvent> indexData = computerData.map(item -> convert(item))
                .name("转换为指标");
        indexData.print();
//        indexData.addSink(KafkaUtil.buildSink(parameterTool))
//                .setParallelism(parameterTool.getInt(PropConst.FLINK_SINK_PARALLELISM))
//                .name("输出结果");

        env.execute();
    }

    public static IndexEvent convert(ComputeModel model) {
        IndexEvent index = new IndexEvent();
        index.setSeq(UUID.randomUUID().toString());
        index.setVal(String.valueOf(model.getCount()));
        index.setBizType("BUSI_COUNT");
        index.setSubBizType("APPLY_COUNT");
        index.setBusiTime(System.currentTimeMillis());
        Map<String, Object> props = new HashMap<>();
        props.put("areaCode", model.getAreaCode());
        props.put("submitChannel", model.getSubmitChannel());
        index.setProps(props);
        return index;
    }

}
