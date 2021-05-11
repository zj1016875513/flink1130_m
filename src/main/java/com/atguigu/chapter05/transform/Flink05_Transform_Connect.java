package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink05_Transform_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 100, 200, 400);
        DataStreamSource<String> s2 = env.fromElements("a", "b", "c");
        
        ConnectedStreams<Integer, String> s12 = s1.connect(s2);
        
        
        SingleOutputStreamOperator<String> result = s12
            /*.map(new CoMapFunction<Integer, String, String>() {
                @Override
                public String map1(Integer value) throws Exception {
                    return value * value + "";
                }
                
                @Override
                public String map2(String value) throws Exception {
                    return value + " " + value;
                }
            });*/
            .process(new CoProcessFunction<Integer, String, String>() {
                @Override
                public void processElement1(Integer value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    out.collect(value + "");
                }
    
                @Override
                public void processElement2(String value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
        
                }
            });
        result.print();
        
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

 */