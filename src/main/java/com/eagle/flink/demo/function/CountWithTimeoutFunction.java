package com.eagle.flink.demo.function;

import com.eagle.flink.demo.model.IndexEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/9 20:45
 * @Version 1.0
 **/
public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, IndexEvent, Tuple2<String, Long>> {
    /**
     * 这个状态是通过 ProcessFunction 维护
     */
    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration configuration) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("ApplyCountWarnState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(IndexEvent indexEvent, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
        // 查看当前计数
        CountWithTimestamp current = state.value();
        if (null == current) {
            current = new CountWithTimestamp();
            current.key = indexEvent.getChannel() + indexEvent.getBizType() + indexEvent.getSubBizType() + indexEvent.getProductCode();
        }
        // 更新状态计数
        current.count++;
        current.lastModified = context.timestamp();
        // 状态回写
        state.update(current);
        // 从当前事件时间开始注册一个60S的定时器
        context.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        // 得到设置这个定时器的键对应的状态
        CountWithTimestamp result = state.value();
        // 检查定时器是过时定时器还是最新定时器
        if (timestamp == result.lastModified + 60000) {
            // emit state on timeout
            out.collect(new Tuple2<>(result.key, result.count));
        }
    }
}

/**
 * 存储在state中的数据类型
 */
class CountWithTimestamp {
    public String key;
    public long count;
    public long lastModified;
}

