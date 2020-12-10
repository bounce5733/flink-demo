package com.eagle.flink.demo.applycount;

import com.eagle.flink.demo.applycount.model.ComputeModel;
import com.eagle.flink.demo.constant.PropConst;
import com.eagle.flink.demo.utils.ExecutionEnvUtil;
import com.eagle.flink.demo.utils.KafkaUtil;
import com.eagle.micro.model.flink.IndexEvent;
import com.eagle.micro.model.flink.IndexSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class App {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<IndexSource> sourceData = KafkaUtil.buildSource(env,"APPLY_COUNT");
        sourceData.name("接收源数据");

        SingleOutputStreamOperator<ComputeModel> computerData = sourceData.keyBy(new MultiKey())
                .window(new TodayWindow())
                .trigger(new EveryoneTrigger())
                .aggregate(new Compute()).name("分组统计进件量");

        SingleOutputStreamOperator<IndexEvent> indexData = computerData.map(item -> convert(item))
                .name("转换为指标");

        indexData.addSink(KafkaUtil.buildSink(parameterTool))
                .setParallelism(parameterTool.getInt(PropConst.FLINK_SINK_PARALLELISM))
                .name("输出结果");

        env.execute("按产品｜地区｜提交渠道分组统计当日进件");
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
