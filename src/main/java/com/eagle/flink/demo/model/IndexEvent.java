package com.eagle.flink.demo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class IndexEvent {

    private int seq;
    private String bizType;
    private String subBizType;
    private String productCode;
    private String val;
    private long timestamp;
    private String channel;
    private int count;
    private Map<String, Object> props;

    public void countPlus() {
        this.count++;
    }
}
