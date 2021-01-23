package com.eagle.flink.demo.union;

import com.eagle.flink.demo.model.CurrencyType;
import com.eagle.flink.demo.model.ExchangeRateInfo;
import com.eagle.flink.demo.source.ExchangeRateDataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2021/1/7 10:55
 * @Version 1.0
 **/
public class App {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<ExchangeRateInfo> cnyToUsd = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.USD, 6.5f), "CNY-USD");

        DataStreamSource<ExchangeRateInfo> cnyToEur = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.EUR, 10.2f), "CNY-EUR");

        DataStreamSource<ExchangeRateInfo> cnyToAud = env.addSource(new ExchangeRateDataSource(CurrencyType.CNY, CurrencyType.AUD, 3.2f), "CNY-AUD");

        DataStream<ExchangeRateInfo> allExchangeRateStream = cnyToUsd.union(cnyToEur).union(cnyToAud);
        allExchangeRateStream.print();

        env.execute("union demo");
    }
}
