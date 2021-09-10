package com.atguigu.chapter07;

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
public class Flink08_Window_No_Key {
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
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum(1).setParallelism(2)
            .print();
        //没有keyby windowAll 会进入一个窗口   发送(a,3) (b,4)  输入之后也会在控制台输出(a,7)
        //非key分区的流上使用window, 如果把并行度强行设置为>1, 则会抛出异常
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
