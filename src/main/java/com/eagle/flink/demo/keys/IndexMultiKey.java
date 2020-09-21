package com.eagle.flink.demo.keys;

import com.eagle.flink.demo.model.IndexEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 分组多个key选择器
 *
 * @Author jiangyonghua
 * @Date 2020/9/8 21:22
 * @Version 1.0
 **/
public class IndexMultiKey implements KeySelector<IndexEvent, Tuple3<String, String, String>> {
    @Override
    public Tuple3<String, String, String> getKey(IndexEvent indexEvent) {
        return new Tuple3<>(
                indexEvent.getProductCode(),
                String.valueOf(indexEvent.getProps().get("areaCode")),
                String.valueOf(indexEvent.getProps().get("submitChannel")));
    }
}
