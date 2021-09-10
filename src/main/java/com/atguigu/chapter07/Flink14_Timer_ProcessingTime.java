package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink14_Timer_ProcessingTime {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                @Override
                public void processElement(Tuple2<String, Long> value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if("a".equals(value.f0)){
                        
                        long now = System.currentTimeMillis();
                        System.out.println("now: " + now);
                        // 注册定时器
                        ctx.timerService().registerProcessingTimeTimer(now + 5000); //到(now+5000)这个时间之后触发定时器
                    }
        
                }
    
                // 当定时器触发的时候, 回调这个方法
                // 参数1: 定时器的触发时间
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    System.out.println("timestamp: " + timestamp);
                    out.collect("定时器触发了...");
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
