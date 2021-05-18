package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink13_Window_Grouped_Tumbling_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));
        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tEnv.executeSql("create table sensor(" +
                            "id string," +
                            "ts bigint," +
                            "vc int, " +
                            "t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss'))," +
                            "watermark for t as t - interval '5' second)" +
                            "with("
                            + "'connector' = 'filesystem',"
                            + "'path' = 'input/sensor.txt',"
                            + "'format' = 'csv'"
                            + ")");
        
        /*tEnv
            .sqlQuery(
                "select " +
                    "id, " +
                    "tumble_start(t, interval '5' second) w_start, " +
                    "tumble_end(t, interval '5' second) w_end, " +
                    "sum(vc) vc_sum " +
                    "from sensor " +
                    "group by id, tumble(t, interval '5' second)"
            )
            .execute()
            .print();*/
    
        /*tEnv
            .sqlQuery(
                "select " +
                    "id, " +
                    "hop_start(t, interval '2' second, interval '5' second) w_start, " +
                    "hop_end(t, interval '2' second, interval '5' second) w_end, " +
                    "sum(vc) vc_sum " +
                    "from sensor " +
                    "group by id, hop(t, interval '2' second, interval '5' second)"
            )
            .execute()
            .print();*/
    
        tEnv
            .sqlQuery(
                "select " +
                    "id, " +
                    "session_start(t, interval '3' second) w_start, " +
                    "session_end(t, interval '3' second) w_end, " +
                    "sum(vc) vc_sum " +
                    "from sensor " +
                    "group by id, session(t, interval '3' second)"
            )
            .execute()
            .print();
        
    }
}
/*
over
 */