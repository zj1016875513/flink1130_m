package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 16:39
 */
public class Flink10_Window_WaterMark_OutOrderness {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);
    
        SingleOutputStreamOperator<String> main = env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));
            
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000)
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            //.allowedLateness(Time.seconds(2))
            .sideOutputLateData(new OutputTag<WaterSensor>("lateData") {})
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    int count = 0;
                    for (WaterSensor ws : elements) {
                        count++;
                    }
                    TimeWindow w = ctx.window();
                    out.collect(
                        "当前Key=" + key
                            + "窗口: [ " + w.getStart() / 1000 + "," + w.getEnd() / 1000 + " ), "
                            + "元素个数: " + count
                    );
                }
            });
    
        main.print("main");
    
        main.getSideOutput(new OutputTag<WaterSensor>("lateData") {}).print("late");
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
保证数据不丢失:
1. 水印
2. 允许迟到
    到了窗口的结束时间(水印)的时候, 这个会进行计算, 但是不关窗
    如果有属于这个窗口的数据到达, 则会重新计算
    迟到也有一个上限

3. 使用侧输出流
 

 */