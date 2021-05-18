package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink01_Table_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
        // 1. 先有table的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 2. 把流转成动态表
        Table table = tEnv.fromDataStream(waterSensorStream);
        // 3. 在动态表上执行连续查询
        //        Table result = table.select("id,ts,vc");   // id=sensor_1  select a as aa,
        Table result = table
            .where($("id").isEqual("sensor_1"))
            .select($("id").as("idd"), $("ts"));
    
        result.execute().print();  // 直接把动态表的数据打印到终端. 为了方便输出
        
        // 4. 把连续查询的结果(动态表) 转成流
//        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);
        
        // 5. 把流sink出去
//        resultStream.print();
        
        //env.execute();  当没有对流左任何操作的时候, 可以把他去掉
        
    }
}
