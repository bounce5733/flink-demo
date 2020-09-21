package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.model.IndexSource;

import java.util.Random;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/6 19:07
 * @Version 1.0
 **/
public class IndexSourceFactory {

    private static final String[] PRODUCT_CODE = {"2016000001000", "2017000001000", "2016000002000", "2018000001000", "2019000001000"};

    private static final String[] AREA_CODE = {"330000", "331000", "340000", "341000"};

    private static final String[] SUBMIT_CHANNEL = {"JD", "YZF", "HB", "XC"};

    public static IndexSource create() {

        return IndexSource.builder().areaCode(AREA_CODE[new Random().nextInt(AREA_CODE.length)])
                .submitChannel(SUBMIT_CHANNEL[new Random().nextInt(SUBMIT_CHANNEL.length)])
                .productCode(PRODUCT_CODE[new Random().nextInt(PRODUCT_CODE.length)])
                .timestamp(System.currentTimeMillis())
                .val(String.valueOf(new Random().nextInt(10)))
                .build();
    }
}
