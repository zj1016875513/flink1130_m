package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink08_Transform_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        DataStreamSource<WaterSensor> s1 = env.fromElements(
//            new WaterSensor("sensor_1", 4L, 40),
            new WaterSensor("sensor_1", 2L, 50),
            new WaterSensor("sensor_1", 1L, 40),
            new WaterSensor("sensor_2", 2L, 20),
//            new WaterSensor("sensor_3", 3L, 30),
            new WaterSensor("sensor_3", 5L, 50));
    
        s1
            .keyBy(ws -> ws.getId())
            .reduce((value1, value2) -> {
                System.out.println("reduce...");
                //value1.setVc(value1.getVc() + value2.getVc());
                value1.setVc(Math.max(value1.getVc(), value2.getVc()));
                return value1;
            })
            .print();
            
         
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
reduce
    之后的流中数据的类型和聚合前必须一致!
    
    1. 如果一个key只有一个值, 则reduce函数不会执行
 
 */