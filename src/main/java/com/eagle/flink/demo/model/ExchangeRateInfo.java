package com.eagle.flink.demo.model;

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 汇率
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 17:06
 * @Version 1.0
 **/
@Data
public class ExchangeRateInfo implements Serializable {

    private String from;
    private String to;
    private Date time;
    private BigDecimal rate;

    public ExchangeRateInfo() {}

    public ExchangeRateInfo(CurrencyType from, CurrencyType to, BigDecimal rate, Date time) {
        this.from = from.name();
        this.to = to.name();
        this.time = time;
        this.rate = rate;
    }

}

