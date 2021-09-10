package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink19_Function_Agg_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        DataStreamSource<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
    
        Table table = tEnv.fromDataStream(waterSensorStream);
    
        // 内联
       /* table
            .groupBy($("id"))
            .select($("id"), call(MySum.class, $("vc")).as("vc_sum"))
            .execute()
            .print();*/
        
        // 注册后再使用
        tEnv.createTemporaryFunction("my_sum", MySum.class);
        table
            .groupBy($("id"))
            .select($("id"), call("my_sum", $("ts"),$("vc")).as("vc_sum"))
            .execute()
            .print();
    }
    
    public static class MyAcc{
        public Double sum = 0D;
    }
    // 泛型一: 最终的结果的类型  泛型2:是累加器
    public static class MySum extends AggregateFunction<Double, MyAcc> {
    
        @Override
        public Double getValue(MyAcc acc) {
            return acc.sum;
        }
    
        @Override
        public MyAcc createAccumulator() {
            return new MyAcc();
        }
        
        public void accumulate(MyAcc acc, Double ... values){
            for (Double value : values) {
                acc.sum += value;
            }
        }
    }
    
   
    
}

/*
hello  HELLO   5
world  WORLD   5
atguigu ATGUIGU 7
...


 */