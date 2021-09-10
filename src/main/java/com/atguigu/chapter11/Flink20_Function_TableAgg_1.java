package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink20_Function_TableAgg_1 {
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
        
        //内练
        
        table
            .groupBy($("id"))
            .flatAggregate(call(MyTableAgg.class, $("vc")))
            .select($("id"), $("f0"), $("f1"))
            .execute()
            .print();
        
    
        
    
    }
    
    public static class FirstSecond{
        public Integer first = 0;
        public Integer second = 0;
    }
    /*
    需求: 提取最高的两个水位值
     */
    public static class MyTableAgg extends TableAggregateFunction<Tuple2<Integer, Integer>, FirstSecond> {
    
        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }
        
        // 对水位进行累加:  计算最大和第二大    在累加器中计算最大值和第二大值
        public void accumulate(FirstSecond acc, Integer v){  // f = 100 s = 80    100  f=120  s=80
            if(v > acc.first){
                acc.second = acc.first;
                acc.first = v;
            }else if(v > acc.second){
                acc.second = v;
            }
        }
        
        // 把结果发射出去

        public void emitValue(FirstSecond acc, Collector<Tuple2<Integer, Integer>> out){
            out.collect(Tuple2.of(acc.first, acc.second));  // 调用一次, 结果就有一行
//            out.collect(Tuple2.of(acc.first, acc.second));
        }
    }
    
}

