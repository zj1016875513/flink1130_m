package com.atguigu;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 16:22
 */
public class Flink01_Transform_Split {
    public static void main(String[] args) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // split
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6, 78, 9);
        
        
        SplitStream<Integer> ss = s1.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                if (value % 2 == 0) {
                    return Arrays.asList("偶数", "0");
                } else {
                
                    return Arrays.asList("奇数","1");
                }
            
            }
        });
    
//        DataStream<Integer> s11 = ss.select("0");
//        s11.print();

        DataStream<Integer> s12 = ss.select("奇数");
        s12.print();
        
        // 侧输出流 替换掉了切割流
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
