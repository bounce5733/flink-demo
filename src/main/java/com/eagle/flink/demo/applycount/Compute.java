package com.eagle.flink.demo.applycount;

import com.eagle.flink.demo.applycount.model.ComputeModel;
import com.eagle.micro.model.flink.IndexSource;
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
public class Compute implements AggregateFunction<IndexSource, ComputeModel, ComputeModel> {

    @Override
    public ComputeModel createAccumulator() {
        return null;
    }

    @Override
    public ComputeModel add(IndexSource in, ComputeModel acc) {
        if (null == acc) {
            return ComputeModel.builder().areaCode(in.getAreaCode())
                    .submitChannel(in.getSubmitChannel()).build();
        } else {
            acc.setCount(acc.getCount() + 1);
        }
        return acc;
    }

    @Override
    public ComputeModel getResult(ComputeModel computeModel) {
        return computeModel;
    }

    @Override
    public ComputeModel merge(ComputeModel acc1, ComputeModel acc2) {
        return null;
    }
}
