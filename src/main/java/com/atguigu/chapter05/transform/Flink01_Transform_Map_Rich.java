package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink01_Transform_Map_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4);
        s1
            
            .map(new RichMapFunction<Integer, Integer>() {
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 做一些初始化的操作 : 连接数据库  执行次数与并行度一致
                    System.out.println("open ....");
                }
    
                @Override
                public Integer map(Integer value) throws Exception {
                    return value * value;
                }
    
                @Override
                public void close() throws Exception {
                    // 程序关闭的时候执行. 执行次数与并行度一致
                    // 做一些关闭资源是操作
                    System.out.println("close...");
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
