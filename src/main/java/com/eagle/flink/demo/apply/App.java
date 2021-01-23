package com.eagle.flink.demo.apply;

import com.eagle.flink.demo.model.Fruit;
import com.eagle.flink.demo.source.FruitDataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * apply 函数是对窗口内的数据做处理的核心方法。这是对10s窗口中的所有元素做过滤，只输出商品名称为苹果的订单
 *
 * @Author jiangyonghua
 * @Date 2021/1/7 13:57
 * @Version 1.0
 **/
public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Fruit> fruitDataStreamSource = env.addSource(new FruitDataSource());

        fruitDataStreamSource.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction<Fruit, Object, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Fruit> values, Collector<Object> out) throws Exception {
                values.forEach(fruit -> {
                    if ("苹果".equals(fruit.getName())) {
                        out.collect(fruit);
                    }
                });
            }
        }).print();

        env.execute("筛选苹果");
    }
}
