package com.eagle.flink.demo.function;

import com.eagle.flink.demo.model.IndexEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 进件量累加器
 *
 * @Author jiangyonghua
 * @Date 2020/9/10 10:02
 * @Version 1.0
 **/
@Slf4j
public class ApplyCountAcc implements AggregateFunction<IndexEvent, IndexEvent, IndexEvent> {

    @Override
    public IndexEvent createAccumulator() {
        return null;
    }

    @Override
    public IndexEvent add(IndexEvent in, IndexEvent acc) {
        if (null == acc) {
            acc = in;
            acc.setVal("1");
        } else {
            acc.setVal(String.valueOf(Integer.valueOf(acc.getVal()) + 1));
        }
        return acc;
    }

    @Override
    public IndexEvent getResult(IndexEvent indexEvent) {
        return indexEvent;
    }

    @Override
    public IndexEvent merge(IndexEvent acc1, IndexEvent acc2) {
        return null;
    }
}
