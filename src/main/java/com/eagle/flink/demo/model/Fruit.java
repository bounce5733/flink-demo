package com.eagle.flink.demo.model;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2021/1/6 17:26
 * @Version 1.0
 **/
@Data
public class Fruit implements Serializable {

    private String name;

    private String currencyType;

    private float price;

    private int num;

    private Date time;

    public Fruit() {}

    public Fruit(String name, CurrencyType currencyType, float price, int num, Date time) {
        this.name = name;
        this.currencyType = currencyType.name();
        this.price = price;
        this.num = num;
        this.time = time;
    }
}
