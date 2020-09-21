package com.eagle.flink.demo.utils;

import com.eagle.flink.demo.constant.BUSI_TYPE_ENUM;
import com.eagle.flink.demo.constant.CHANNEL_ENUM;
import com.eagle.flink.demo.constant.SUB_BUSI_TYPE_ENUM;
import com.eagle.flink.demo.model.IndexEvent;
import com.eagle.flink.demo.model.IndexSource;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jiangyonghua
 * @Date 2020/9/9 20:33
 * @Version 1.0
 **/
public class BeanUtil {

    public static IndexEvent convert(IndexSource indexSource, CHANNEL_ENUM channel, BUSI_TYPE_ENUM busiType, SUB_BUSI_TYPE_ENUM subBusiType) {
        IndexEvent index = new IndexEvent();
        index.setSeq(indexSource.getSeq());
        index.setVal(indexSource.getVal());
        index.setProductCode(indexSource.getProductCode());
        index.setChannel(channel.name());
        index.setBizType(busiType.name());
        index.setSubBizType(subBusiType.name());
        index.setTimestamp(indexSource.getTimestamp());
        Map<String, Object> props = new HashMap<>();
        props.put("areaCode", indexSource.getAreaCode());
        props.put("submitChannel", indexSource.getSubmitChannel());
        index.setProps(props);
        return index;
    }
}
