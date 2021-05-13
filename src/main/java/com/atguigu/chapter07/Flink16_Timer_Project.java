package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 11:29
 */
public class Flink16_Timer_Project {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
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
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                boolean isFirst = true;
                Integer lastVc = 0;
                long timerTs;
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 当第一条数据来的时候,注册一个定时器
                    if (isFirst) {
                        System.out.println("第一条...");
                        isFirst = false;  // 第一条数据进来之后, 先把isFirst置为false, 后面的数据进来之后就不是第一条了
                        timerTs = ctx.timestamp() + 5000;
                        ctx.timerService().registerEventTimeTimer(timerTs);
                    } else {
                        if (value.getVc() <= lastVc) {
                            System.out.println("水位没有上升");
                            // 出现水位没有上升:    取消定时器
                            // 1. 上次注册的定时器取消
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            // 2. 重新注册一个新的定时器
                            timerTs = ctx.timestamp() + 5000;
                            ctx.timerService().registerEventTimeTimer(timerTs);
                        }else{
                            System.out.println("水位上升");
                        }
                    }
                    
                    // 不管水位是否上升, 都应该更新上次的 水位值
                    lastVc = value.getVc();
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    out.collect("传感器: " + ctx.getCurrentKey() + " 连续5s水位上升, 红色预警");
                    isFirst = true;
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
