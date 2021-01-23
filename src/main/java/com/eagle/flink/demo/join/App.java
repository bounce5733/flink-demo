package com.eagle.flink.demo.join;

import com.eagle.flink.demo.model.ExchangeRateInfo;
import com.eagle.flink.demo.model.Fruit;
import com.eagle.flink.demo.source.ExchangeRateDataSource;
import com.eagle.flink.demo.source.FruitDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 16:59
 * @Version 1.0
 **/
@Slf4j
public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000L); // 设置周期发送水印间隔

        SingleOutputStreamOperator<Fruit> fruitDataStream = env.addSource(new FruitDataSource())
                .assignTimestampsAndWatermarks(((WatermarkStrategy<Fruit>) context -> new WatermarkGenerator<Fruit>() {
                    private long maxTimestamp;
                    private long delay = 100;

                    @Override
                    public void onEvent(Fruit event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(maxTimestamp, event.getTime().getTime());
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - delay));
                    }
                }).withTimestampAssigner((event, time) -> event.getTime().getTime()));

        SingleOutputStreamOperator<ExchangeRateInfo> usdToCnyDataStream = env.addSource(new ExchangeRateDataSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ExchangeRateInfo>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner((event, time) -> event.getTime().getTime()));

        fruitDataStream.join(usdToCnyDataStream).where((KeySelector<Fruit, String>) value -> value.getCurrencyType())
                .equalTo((KeySelector<ExchangeRateInfo, String>) value -> value.getFrom())
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //将订单金额人民币转换为美元
                .apply((JoinFunction<Fruit, ExchangeRateInfo, String>) (first, second) -> {
                    return "¥：" + first.getPrice() * first.getNum() + " = $:" + new BigDecimal(first.getPrice() * first.getNum())
                            .divide(second.getRate(), 2, BigDecimal.ROUND_HALF_UP);
                })
                .print();

        env.execute("水果市场汇率转换");
    }
}
