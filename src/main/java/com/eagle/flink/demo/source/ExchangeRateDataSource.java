package com.eagle.flink.demo.source;

import com.eagle.flink.demo.model.CurrencyType;
import com.eagle.flink.demo.model.ExchangeRateInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 汇率源
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 16:59
 * @Version 1.0
 **/
public class ExchangeRateDataSource implements SourceFunction<ExchangeRateInfo> {

    private volatile boolean isRunning = true;

    private ExchangeRateInfo exchangeRateInfo;

    public ExchangeRateDataSource(){
        this.exchangeRateInfo = new ExchangeRateInfo(CurrencyType.CNY, CurrencyType.USD, new BigDecimal(6.8), new Date());
    }

    public ExchangeRateDataSource(CurrencyType from, CurrencyType to, float rate) {
        this.exchangeRateInfo = new ExchangeRateInfo(from, to, new BigDecimal(rate), new Timestamp(new Date().getTime()));
    }

    @Override
    public void run(SourceContext<ExchangeRateInfo> ctx) throws Exception {
        while (isRunning) {
            Thread.sleep(2000);
            ctx.collect(this.exchangeRateInfo);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
