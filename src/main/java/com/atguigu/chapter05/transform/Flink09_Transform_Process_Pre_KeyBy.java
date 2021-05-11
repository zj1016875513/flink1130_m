package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink09_Transform_Process_Pre_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);
    
        /*s1
            .process(new ProcessFunction<Integer, Integer>() {
                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    if (value > 3) {
                        out.collect(value);
                        out.collect(value * value);
                    }
                }
            })
            .print();*/
        
        s1
            .keyBy(x -> 1)
            /*.process(new ProcessFunction<Integer, Integer>() {
                Integer sum = 0;
                
                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    sum += value;
                    out.collect(sum);
                }
            })*/
            .process(new KeyedProcessFunction<Integer, Integer, Integer>() {
                
                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    
        
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
