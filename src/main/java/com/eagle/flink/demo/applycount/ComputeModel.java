package com.eagle.flink.demo.applycount;/**
 * TODO
 *
 * @author: yh.jiang
 * @time: 2020/12/10 10:13
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 计算模型
 *
 * @Author jiangyonghua
 * @Date 2020/12/10 10:13
 * @Version 1.0
 **/
@Data
@Builder
@AllArgsConstructor
public class ComputeModel {

    private String areaCode;

    private String submitChannel;

    private int count;

}
