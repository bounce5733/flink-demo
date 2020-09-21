package com.eagle.flink.demo;

import com.eagle.flink.demo.constant.BUSI_TYPE_ENUM;
import com.eagle.flink.demo.constant.CHANNEL_ENUM;
import com.eagle.flink.demo.constant.PropConst;
import com.eagle.flink.demo.constant.SUB_BUSI_TYPE_ENUM;
import com.eagle.flink.demo.function.PutoutAmountAcc;
import com.eagle.flink.demo.keys.IndexMultiKey;
import com.eagle.flink.demo.model.IndexEvent;
import com.eagle.flink.demo.model.IndexSource;
import com.eagle.flink.demo.trigger.EveryoneTrigger;
import com.eagle.flink.demo.utils.BeanUtil;
import com.eagle.flink.demo.utils.ExecutionEnvUtil;
import com.eagle.flink.demo.utils.KafkaUtil;
import com.eagle.flink.demo.window.TodayWindow;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class TodayPutoutAmount {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<IndexSource> dataSource = KafkaUtil.buildSource(env, "TodayPutoutAmountGroupId");
        dataSource.name("接收数据");

        SingleOutputStreamOperator<IndexEvent> dataTrans = dataSource.setParallelism(1)
                .map(indexSource -> BeanUtil.convert(indexSource, CHANNEL_ENUM.CREDIT, BUSI_TYPE_ENUM.BUSI_AMOUNT, SUB_BUSI_TYPE_ENUM.PUTOUT_AMOUNT))
                .name("指标生成");

        SingleOutputStreamOperator<IndexEvent> groupAcc = dataTrans.keyBy(new IndexMultiKey())
                .window(new TodayWindow())
                .trigger(new EveryoneTrigger())
                .aggregate(new PutoutAmountAcc())
                .name("分组累加出账额");

        groupAcc.addSink(KafkaUtil.buildSink(parameterTool))
                .setParallelism(parameterTool.getInt(PropConst.FLINK_SINK_PARALLELISM))
                .name("输出结果");

        env.execute("按产品｜地区｜提交渠道分组统计当日额度");
    }
}
