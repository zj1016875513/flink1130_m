package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 10:19
 */
public class Flink02_StreamWordCount_Bounded {
    public static void main(String[] args) throws Exception {
        // 流式api, 处理有界流(从文件读取数据)
        // 1. 获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 从文件获取流
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");
        // 3. 对流做各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = lineDS
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value,
                                    Collector<String> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(word);
                    }
                }
            })
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String word) throws Exception {
                    return Tuple2.of(word, 1L);
                }
            });
    
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneDS
            .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                @Override
                public String getKey(Tuple2<String, Long> t) throws Exception {
                    return t.f0;
                }
            })
            .sum(1);
        // 4. 输出流数据
        result.print();
        // 5. 执行流环境
        env.execute();
        
    }
}
