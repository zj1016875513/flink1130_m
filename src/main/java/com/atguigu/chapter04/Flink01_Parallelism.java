package com.atguigu.chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 10:32
 */
public class Flink01_Parallelism {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        env.disableOperatorChaining();
        
        env
            .socketTextStream("hadoop162", 9999)
            .filter(value -> true)
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                }
            })
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    return Tuple2.of(value, 1L);
                }
            })
            // keyBy不能设置并行度
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        env.execute();
    }
}
/*
如何设置算子的并行度:
0. 默认并行度:
    cpu的核心数

1. 在配置文件中(全局配置) flink-conf.yml
    parallelism.default: 1
    
2. 在提交任务的时候通过参数设置
    -p 2

3. 在代码中, 通过env环境来进行设置

4. 单独的给每个算子设置


.startNewChain()
    让当前的算子单独起一个操作链
.disableChaining()
    当前算子禁用操作链
env.disableOperatorChaining();
    在整个应用整体禁用操作链
    
 */