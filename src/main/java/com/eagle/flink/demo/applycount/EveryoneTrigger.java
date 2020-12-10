package com.eagle.flink.demo.applycount;

import com.eagle.micro.model.flink.IndexSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 每个元素触发
 *
 * @Author jiangyonghua
 * @Date 2020/9/10 11:10
 * @Version 1.0
 **/
@Slf4j
public class EveryoneTrigger extends Trigger<IndexSource, TimeWindow> {

    @Override
    public TriggerResult onElement(IndexSource indexSource, long l, TimeWindow timeWindow, TriggerContext triggerContext) {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
        return null;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }
}
