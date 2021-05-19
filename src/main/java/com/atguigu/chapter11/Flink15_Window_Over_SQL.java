package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink15_Window_Over_SQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table sensor(" +
                            "id string, " +
                            "ts bigint, " +
                            "vc int, " +
                            "et as to_timestamp(from_unixtime(ts) ), " +  // 这个地方ts必须是s
                            "watermark for et as et - interval '4' second" +
                            ")with(" +
                            "   'connector' = 'filesystem', " +
                            "   'path' = 'input/sensor.txt', " +
                            "   'format' = 'csv' " +
                            ")");
        
       /* tEnv
            .sqlQuery("select" +
                          " id, ts, vc, " +
//                          " sum(vc) over(partition by id order by et rows between unbounded preceding and current row) vc_sum1, " +
//                          " sum(vc) over(partition by id order by et rows between 1 preceding and current row) vc_sum1, " +
//                          " max(vc) over(partition by id order by et rows between 1 preceding and current row) vc_sum2 " +
                          " sum(vc) over(partition by id order by et range between interval '1' second preceding and current row) vc_sum2 " +
                          "from sensor")
            .execute()
            .print();*/
        
        tEnv
            .sqlQuery("select" +
                          " id, ts, vc, " +
                          " sum(vc) over w sum_vc, " +
                          " max(vc) over w max_vc " +
                          "from default_catalog.default_database.sensor " +
                          "window w as (partition by id order by et rows between unbounded preceding and current row) ")
            .execute()
            .print();
        
    }
}
