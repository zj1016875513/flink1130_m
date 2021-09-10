package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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

//        tEnv.toRetractStream(result,Row.class) .filter(x -> x.f0).map(x -> x.f1).print();
        //先过滤出f0为true的数据，再选择f1的数据，这里的f1指除掉第一列的布尔值之后的后面的所有列的值
//        效果：sensor_1,10
//             sensor_1,30
//             sensor_2,30
//             sensor_1,70
//             sensor_1,120
//             sensor_2,90

        //.filter(x -> x.f0) 为过滤true的数据
        //.filter(x -> !x.f0) 为过滤false的数据
        tEnv.toRetractStream(result,Row.class).filter(x -> x.f0).print();
//      效果：  (true,sensor_1,10)
//             (true,sensor_1,30)
//             (true,sensor_2,30)
//             (true,sensor_1,70)
//             (true,sensor_1,120)
//             (true,sensor_2,90)

//        tEnv.toRetractStream(result,Row.class) .print();
        env.execute();
//        效果：(true,sensor_1,10)
//        (false,sensor_1,10)
//        (true,sensor_1,30)
//        (true,sensor_2,30)
//        (false,sensor_1,30)
//        (true,sensor_1,70)
//        (false,sensor_1,70)
//        (true,sensor_1,120)
//        (false,sensor_2,30)
//        (true,sensor_2,90)
        // 5. 把流sink出去
//        resultStream.print();
        
//        env.execute();  //当没有对流左任何操作的时候, 可以把他去掉
        
    }
}
