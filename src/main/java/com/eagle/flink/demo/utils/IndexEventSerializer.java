package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.constant.GlobalConst;
import com.eagle.flink.demo.model.IndexEvent;
import com.google.gson.Gson;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/8 09:51
 * @Version 1.0
 **/
public class IndexEventSerializer implements Serializer<IndexEvent> {

    @SneakyThrows
    @Override
    public byte[] serialize(String s, IndexEvent indexEvent) {
        return new Gson().toJson(indexEvent).getBytes(GlobalConst.CODING);
    }
}
