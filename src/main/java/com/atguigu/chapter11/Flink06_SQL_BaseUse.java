package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink06_SQL_BaseUse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60));
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(waterSensorStream);
        // select * from t where ...
        // 第一种查询方式: 使用未注册的表  就是把table对象拼接在sql语句中
//        Table result = tableEnv.sqlQuery("select * from " + table + " where vc > 20");   """ """
        /*Table result = tableEnv.sqlQuery("select " +
                                             "  id, " +
                                             "  sum(vc) vc_sum " +
                                             " from " + table +
                                             " where vc > 20 " +
                                             " group by id");*/
        
        // 第二种查询方式: 使用已注册   参数1: 表名 参数2: 表对象
        tableEnv.createTemporaryView("sensor", table);
    
        Table result = tableEnv.sqlQuery("select " +
                                             " * " +
                                             "from sensor");
    
        result.execute().print();
    
    }
}
/*
sqlQuery()  执行查询语句
executeSql()   ddl 语句(建表)  dml 语句(增删改)

 */