package com.atguigu.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink07_Window_AggregateFunction {
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
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .aggregate(
                new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    
                    // 创建累加器 , 给累加器初始化值
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    
                    // 根据输入的元素, 给累计器进行累加
                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        return accumulator + 1;
                    }
                    
                    // 当窗口关闭的时候, 返回最终的结果
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }
                    
                    // 合并累加器
                    // 这个方法只有在session窗口才会执行, 其他的窗口不会执行
                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Long> elements,
                                        Collector<String> out) throws Exception {
                        Long count = elements.iterator().next();
                        out.collect(key + "_" + count);
                    }
                }
            )
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
