package com.eagle.flink.demo.minby;

import com.eagle.flink.demo.model.Fruit;
import com.eagle.flink.demo.source.FruitDataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * min只返回计算的最小值，而最小值对应的其他数据不保证正确。
 * minBy返回计算的最小值，并且最小值对应的其他数据是保证正确的。
 *
 * @Author jiangyonghua
 * @Date 2021/1/7 13:45
 * @Version 1.0
 **/
public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Fruit> fruitSourceStream = env.addSource(new FruitDataSource());
        fruitSourceStream.timeWindowAll(Time.seconds(5)).minBy("price").print();

        env.execute("最便宜水果");
    }
}
