package com.atguigu.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/19 14:50
 */
public class Flink_SQL_Project_TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        tEnv.executeSql("create table user_behavior(" +
                            "user_id bigint, " +
                            "item_id bigint, " +
                            "category_id int, " +
                            "behavior string, " +
                            "ts bigint," +
                            "et as to_timestamp(from_unixtime(ts)), " +
                            "watermark for et as et - interval '2' second " +
                            ")with(" +
                            "   'connector' = 'filesystem', " +
                            "   'path' = 'input/UserBehavior.csv', " +
                            "   'format' = 'csv' " +
                            ")");
        
        // 1. 使用滑窗, 计算每个商品的点击量
        
        Table t1 = tEnv.sqlQuery("select " +
                                     "item_id, " +
                                     "hop_end(et, interval '5' minute, interval '1' hour) w_end, " +
                                     "count(*) item_count " +
                                     "from user_behavior " +
                                     "where behavior='pv' " +
                                     "group by item_id, hop(et, interval '5' minute, interval '1' hour)");
        tEnv.createTemporaryView("t1", t1);
        
        // 2. 使用over窗口, 进行排序 row_number rank dense_rank
        Table t2 = tEnv.sqlQuery("select " +
                                     "*, " +
                                     "row_number() over(partition by w_end order by item_count desc) rk " +
                                     "from t1");
        tEnv.createTemporaryView("t2", t2);
        
        // 3. 过滤出top3
        Table t3 = tEnv.sqlQuery("select " +
                                     "* " +
                                     "from t2 " +
                                     "where rk<=3");

        t3.execute().print();
        
        // 4. 数据写出的到mysql
//        tEnv.executeSql("create table hot_item(" +
//                            "item_id bigint, " +
//                            "w_end timestamp(3), " +
//                            "item_count bigint," +
//                            "rk bigint, " +
//                            "primary key (w_end, rk) NOT ENFORCED" +
//                            ")with(" +
//                            "   'connector' = 'jdbc', " +
//                            "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql?useSSL=false', " +
//                            "   'table-name' = 'hot_item', " +
//                            "   'username' = 'root', " +
//                            "   'password' = 'aaaaaa' " +
//                            ")");
//        t3.executeInsert("hot_item");
    }
}
