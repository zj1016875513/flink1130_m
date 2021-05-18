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
public class Flink02_Table_BaseUse_Agg {
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
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(waterSensorStream);
        
        // select id, sum(vc) from t where vc>=10 group by id
        /*Table result = table
            .where($("vc").isGreaterOrEqual(10))
            .groupBy($("id"))
            .aggregate($("vc").sum().as("vc_sum"))
            .select($("id"), $("vc_sum"));*/
    
        Table result = table
            .where($("vc").isGreaterOrEqual(10))
            .groupBy($("id"))
            .select($("id"), $("vc").sum().as("vc_sum"));
        
       
        
        result.execute().print();
        
        // 4. 把连续查询的结果(动态表) 转成流
//        DataStream<Tuple2<Boolean, Row>> resultStream = tEnv.toRetractStream(result, Row.class);
    
        // 5. 把流sink出去
//        resultStream.print();
        
//        env.execute();  //当没有对流左任何操作的时候, 可以把他去掉
        
    }
}
