package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink01_Transform_Map {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4);
        s1
            /* .map(new MapFunction<Integer, Integer>() {
                 @Override
                 public Integer map(Integer value) throws Exception {
                     return value * value;
                 }
             })*/
            //            .map(v -> v * v)
            .map(new MapFunction<Integer, Integer>() {
                @Override
                public Integer map(Integer value) throws Exception {
                    // 连接数据
                    // 使用
                    // 关闭
                    
                    return value * value;
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
