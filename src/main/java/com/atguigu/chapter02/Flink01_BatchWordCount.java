package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 9:59
 */
public class Flink01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 批处理
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 使用执行环境从文件读取数据(source)
        DataSource<String> sourceDS = env.readTextFile("input/words.txt");
        // 3. 各种转换(transform)
        // 3.1 "a b c" -> "a" "b" "c"
        FlatMapOperator<String, String> wordDS = sourceDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line,
                                Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        // 3.2 a -> ("a", 1)  b -> ("b", 1) ...
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });
        
        // 3.3 分组, 然后聚合
        AggregateOperator<Tuple2<String, Long>> result = wordAndOneDS
            .groupBy(0)
            .sum(1);
        
        // 4. 输出(sink)
        result.print();
        
    }
}
/*
spark:
    1. 创建一个上下文对象
    
    2. 从数据源读取数据
    
    3. 做各种转换
    
    4. 有个行动算子
    
    5. 启动上下文

 */
