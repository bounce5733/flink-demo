package com.eagle.flink.demo.broadcast;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.LocalTime;
import java.util.*;

/**
 * 每分钟广播源
 *
 * @Author jiangyonghua
 * @Date 2020/12/22 14:20
 * @Version 1.0
 **/
@Slf4j
public class MinuteBroadcastSource extends RichParallelSourceFunction<String> {

    private volatile boolean isRun;
    private volatile int lastUpdateMin = -1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRun = true;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRun) {
            LocalTime time = LocalTime.now();
            int min = time.getMinute();
            if (min != lastUpdateMin) {
                lastUpdateMin = min;
                String configStr = new Gson().toJson(readConfigs());
                log.info("发送配置：{}", configStr);
                ctx.collect(configStr);
            }
            Thread.sleep(1000);
        }
    }

    private static Set<String> readConfigs() {
        //这里读取配置信息
        Set<String> firstConfig = new HashSet<>();
        firstConfig.add("341000");
        Set<String> secondConfig = new HashSet<>();
        secondConfig.add("330000");
        secondConfig.add("331000");
        List<Set<String>> configs = new ArrayList<>();
        configs.add(firstConfig);
        configs.add(secondConfig);
        return configs.get(new Random().nextInt(2));
    }


    @Override
    public void cancel() {
        isRun = false;
    }
}

