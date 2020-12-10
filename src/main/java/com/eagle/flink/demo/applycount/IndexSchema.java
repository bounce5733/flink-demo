package com.eagle.flink.demo.applycount;

import com.eagle.micro.model.flink.IndexEvent;
import com.eagle.micro.model.flink.IndexSource;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.Charset;

public class IndexSchema implements DeserializationSchema<IndexSource>, SerializationSchema<IndexEvent> {

    private static final Gson gson = new Gson();

    @Override
    public IndexSource deserialize(byte[] bytes) {
        return gson.fromJson(new String(bytes), IndexSource.class);
    }

    @Override
    public boolean isEndOfStream(IndexSource indexSource) {
        return false;
    }

    @Override
    public TypeInformation<IndexSource> getProducedType() {
        return TypeInformation.of(IndexSource.class);
    }

    @Override
    public byte[] serialize(IndexEvent indexEvent) {
        return new Gson().toJson(indexEvent).getBytes(Charset.forName("UTF-8"));
    }
}
