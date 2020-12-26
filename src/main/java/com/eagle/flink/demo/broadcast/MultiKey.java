package com.eagle.flink.demo.broadcast;

import com.eagle.micro.model.flink.IndexSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 分组多个key选择器
 *
 * @Author jiangyonghua
 * @Date 2020/9/8 21:22
 * @Version 1.0
 **/
public class MultiKey implements KeySelector<IndexSource, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> getKey(IndexSource indexSource) {
        return new Tuple2<>(
                String.valueOf(indexSource.getAreaCode()),
                String.valueOf(indexSource.getSubmitChannel()));
    }
}
