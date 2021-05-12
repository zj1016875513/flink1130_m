package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink07_Window_AggregateFunction_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    String[] data = value.split(",");
                    return Tuple2.of(data[0], Long.valueOf(data[1]));
                }
            })
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Integer>, Double>() {
                @Override
                public Tuple2<Long, Integer> createAccumulator() {
                    System.out.println("createAccumulator...");
                    return Tuple2.of(0L, 0);
                }
                
                @Override
                public Tuple2<Long, Integer> add(Tuple2<String, Long> value,
                                                 Tuple2<Long, Integer> acc) {
                    System.out.println("add ....");
                    return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
                }
                
                @Override
                public Double getResult(Tuple2<Long, Integer> acc) {
                    System.out.println("getResult... ");
                    return acc.f0 * 1.0 / acc.f1;
                }
                
                @Override
                public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                    System.out.println("merge...");
                    return null;
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
