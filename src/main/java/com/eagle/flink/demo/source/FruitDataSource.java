package com.eagle.flink.demo.source;

import com.eagle.flink.demo.model.CurrencyType;
import com.eagle.flink.demo.model.Fruit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 水果数据源
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 17:26
 * @Version 1.0
 **/
public class FruitDataSource implements SourceFunction<Fruit> {

    private volatile boolean isRunning = true;
    private static final List<Tuple2<String, Float>> FRUITS = new ArrayList<>();
    private Random ran = new Random();

    static {
        FRUITS.add(new Tuple2("苹果", 5.1f));
        FRUITS.add(new Tuple2("西瓜", 2.3f));
        FRUITS.add(new Tuple2("葡萄", 3.0f));
        FRUITS.add(new Tuple2("香蕉", 3.4f));
    }

    @Override
    public void run(SourceContext<Fruit> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(1000);
            int index = ran.nextInt(3);
            ctx.collect(new Fruit(FRUITS.get(index).f0, CurrencyType.CNY, FRUITS.get(index).f1, ran.nextInt(10), new Date()));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
