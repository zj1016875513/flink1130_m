package com.atguigu.chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 10:19
 */
public class Flink02_StreamWordCount_UnBounded_Lambda {
    public static void main(String[] args) throws Exception {
        // 流式api, 处理有界流(从文件读取数据)
        // 1. 获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 从文件获取流
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop162", 9999);
        // 3. 对流做各种转换
        SingleOutputStreamOperator<String> wordsDS = lineDS
            .flatMap((String value, Collector<String> out) -> {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            })
            .returns(Types.STRING);
        
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = wordsDS
            .map(word -> Tuple2.of(word, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG));
        
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneDS
            .keyBy(t -> t.f0)
            .sum(1);
        // 4. 输出流数据
        result.print();
        // 5. 执行流环境
        env.execute();
        
    }
}
/*
Lambda: 拉姆达表达式 1.8新增功能 函数式编程

什么时候可以用:
    如果接受的类型是接口, 并且这个接口只有一个抽象函数(可以有默认函数)

*/