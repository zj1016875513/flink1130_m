package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink04_Table_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("tc", DataTypes.INT());
        
        tEnv
            .connect(
                new Kafka()
                    .version("universal")
                    .property("bootstrap.servers", "hadoop162:9092")
                    .property("group.id", "atguigu")
                    .topic("s1")
                    .startFromLatest()
            )
//            .withFormat(new Csv().fieldDelimiter(','))
            .withFormat(new Json())
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        Table sensor = tEnv.from("sensor");
        
        sensor.execute().print();
        
    }
}
