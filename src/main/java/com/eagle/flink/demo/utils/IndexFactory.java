package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.model.IndexEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/6 19:07
 * @Version 1.0
 **/
public class IndexFactory {

    private static final String[] BIZ_TYPE = {"BUSI_COUNT", "BUSI_AMOUNT", "BUSI_WARN"};

    private static final String[] SUB_BIZ_TYPE = {"APPLY_COUNT", "PUTOUT_COUNT", "PUTOUT_AMOUNT", "WITHDRAW_COUNT", "WITHDRAW_AMOUNT"};

    private static final String[] CHANNEL = {"credit", "rpf", "irpf", "acl"};

    private static final String[] PRODUCT_CODE = {"2016000001000", "2017000001000", "2016000002000", "2018000001000", "2019000001000"};

    private static final String[] AREA_CODE = {"330000", "331000", "340000", "341000"};

    private static final String[] SUBMIT_CHANNEL = {"JD", "YZF", "HB", "XC"};

    public static IndexEvent create() {
        Map<String, Object> props = new HashMap<>();
        props.put("areaCode", AREA_CODE[new Random().nextInt(AREA_CODE.length)]);
        props.put("submitChannel", SUBMIT_CHANNEL[new Random().nextInt(SUBMIT_CHANNEL.length)]);
        return IndexEvent.builder().bizType(BIZ_TYPE[new Random().nextInt(BIZ_TYPE.length)])
                .subBizType(SUB_BIZ_TYPE[new Random().nextInt(SUB_BIZ_TYPE.length)])
                .channel(CHANNEL[new Random().nextInt(CHANNEL.length)])
                .productCode(PRODUCT_CODE[new Random().nextInt(PRODUCT_CODE.length)])
                .timestamp(System.currentTimeMillis())
                .val(String.valueOf(new Random().nextInt(10)))
                .props(props)
                .build();
    }
}
