package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink07_Transform_RollAgg {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        DataStreamSource<WaterSensor> s1 = env.fromElements(
            new WaterSensor("sensor_1", 4L, 40),
            new WaterSensor("sensor_1", 2L, 50),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 2L, 20),
            new WaterSensor("sensor_3", 3L, 30),
            new WaterSensor("sensor_3", 5L, 50));
        // select  id, sum(..), 常量 from t1 group by id
        s1
            .keyBy(ws -> ws.getId())
            //            .sum("vc")
            //            .max("vc")
            //            .min("vc")
//            .maxBy("vc", false)
            .minBy("vc", false)
            .print();
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
1. 使用sum max min的必须保证字段的值是数字类型
2. 在聚合的时候, 除了分组字段和聚合字段, 默认情况下其他的字段的值和第一个元素保持一致

3. maxBy(, boolean)
    boolean如果是true, 则当两个值相等的实时, 其他的字段选择第一个值
    boolean如果是false, 则当两个值相等的实时, 其他的字段选择最新的那个
    
    如果值不等, 其他的字段选择的是随着最大或者最小来选
 */