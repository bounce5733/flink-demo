package com.eagle.flink.demo.function.apply;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 15:28
 * @Version 1.0
 **/
public class App {

    private static final String[] FRUITS = {"苹果", "香蕉", "葡萄", "西瓜"};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while(isRunning) {
                    Thread.sleep(1000);
                    ctx.collect(Tuple2.of(FRUITS[random.nextInt(4)], random.nextInt(100)));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }, "水果批发");

        orderSource.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction<Tuple2<String, Integer>, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Object> out) throws Exception {
                values.forEach(fruit -> {
                    if (fruit.f0.equals("苹果")) {
                        out.collect(fruit);
                    }
                });
            }
        }).print();

        env.execute("水果市场");
    }
}
