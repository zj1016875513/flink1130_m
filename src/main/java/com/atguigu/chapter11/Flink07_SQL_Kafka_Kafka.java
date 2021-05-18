package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink07_SQL_Kafka_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 建立一个表, 和source关联起来
        tableEnv.executeSql("create table sensor(" +
                                "   id string, " +
                                "   ts bigint, " +
                                "   vc int" +
                                ")with(" +
                                "   'connector' = 'kafka'," +
                                "   'topic' = 's1',  " +
                                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092',  " +
                                "   'properties.group.id' = 'abc',  " +
                                "   'scan.startup.mode' = 'latest-offset',  " +
                                "   'format' = 'csv'  " +
                                ")");
        
        // sink表
        tableEnv.executeSql("create table abc(" +
                                "   id string, " +
                                "   vc int" +
                                ")with(" +
                                "   'connector' = 'kafka'," +
                                "   'topic' = 's2',  " +
                                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092',  " +
                                "   'format' = 'json',  " +
                                "   'sink.partitioner' = 'round-robin'  " +
                                ")");
        
        Table table = tableEnv.sqlQuery("select id, vc from sensor");
        // 把table的数据写入到sink表有两种写法:
        // 1. 直接调用table的方法
        //        table.executeInsert("abc");
        // 2. 使用 sql语句:  insert into ... select ...
        tableEnv.createTemporaryView("a", table);
        tableEnv.executeSql("insert into abc select * from a");
        
    }
}
/*
sqlQuery()  执行查询语句
executeSql()   ddl 语句(建表)  dml 语句(增删改)

 */