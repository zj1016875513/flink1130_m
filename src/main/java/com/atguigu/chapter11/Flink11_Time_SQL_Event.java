package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink11_Time_SQL_Event {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
      
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table sensor(" +
                            "id string, " +
                            "ts bigint, " +
                            "vc int, " +
                            "et as to_timestamp(from_unixtime(ts) ), " +  // 这个地方ts必须是s
                            "watermark for et as et - interval '2' second" +
                            ")with(" +
                            "   'connector' = 'filesystem', " +
                            "   'path' = 'input/sensor.txt', " +
                            "   'format' = 'csv' " +
                            ")");
        
    
        tEnv.sqlQuery("select * from sensor").execute().print();
        
    }
}
