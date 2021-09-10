package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink14_Window_Over_Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
            .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                                  new WaterSensor("sensor_1", 3000L, 20),
                                  new WaterSensor("sensor_1", 3000L, 40),
                                  new WaterSensor("sensor_1", 5000L, 50),
                                  new WaterSensor("sensor_2", 6000L, 30),
                                  new WaterSensor("sensor_2", 7000L, 60),
                                  new WaterSensor("sensor_1", 9000L, 60))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
            );
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv
            .fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));
        
        OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w");//当前行和上面所有行
//        OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(1L)).as("w"); // 当前行与上一行求和
//        OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w");  // 当前行与前两秒求和
//        OverWindow w = Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_RANGE).as("w");//当前分区不限窗口范围
        table
            .window(w)
            .select($("id"), $("ts"), $("vc").sum().over($("w")))
            .execute()
            .print();
        
    }
}
/*

select
    id,
    sum(vc) over(partition by w order by ts)

 */