package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink16_Hive {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        // 1. 创建hiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog("my_hive", "flink1130", "input/");
        // 2. 注册
        tEnv.registerCatalog("my_hive", hiveCatalog);
        
        // 3. 设置默认的catalog和默认的数据库
        tEnv.useCatalog("my_hive");
        tEnv.useDatabase("flink1130");
        
        // 4. 直接从hive中读取数据
        
        tEnv.sqlQuery("select * from person").execute().print();
        
        
    }
}
