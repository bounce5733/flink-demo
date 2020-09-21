package com.eagle.flink.demo.keys;

import com.eagle.flink.demo.model.IndexEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * 分组单个key选择器
 *
 * @Author jiangyonghua
 * @Date 2020/9/8 21:22
 * @Version 1.0
 **/
public class IndexKey implements KeySelector<IndexEvent, Tuple1<String>> {
    @Override
    public Tuple1<String> getKey(IndexEvent indexEvent) {
        return new Tuple1<>(
                indexEvent.getChannel());
    }
}
