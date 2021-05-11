package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink06_Transform_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 100, 200, 400);
        DataStreamSource<Integer> s2 = env.fromElements(10, 20);
        DataStreamSource<Integer> s3 = env.fromElements(100, 200, 300);
    
        DataStream<Integer> s123 = s1.union(s2, s3);
    
        s123.print();
        
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
1. connect 只能把两个流连起来
2. connect连接的两个流的数据类型可以不一致

3. union可以合并多个流
4. union连接的流的数据类型必须一致

 */