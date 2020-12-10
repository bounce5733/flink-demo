package com.eagle.flink.demo.applycount;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;

/**
 * 当天(0-24)时间窗口
 *
 * @Author jiangyonghua
 * @Date 2020/9/9 21:07
 * @Version 1.0
 **/
public class TodayWindow extends WindowAssigner<Object, TimeWindow> {

    // 窗口分配的主要方法，需要为每一个元素指定所属的分区
    @Override
    public Collection<TimeWindow> assignWindows(Object t, long l, WindowAssignerContext windowAssignerContext) {
        LocalDateTime now = LocalDateTime.now();
        long start = now.with(LocalTime.MIN).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        long end = now.with(LocalTime.MAX).toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return Collections.singletonList(new TimeWindow(start, end));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
