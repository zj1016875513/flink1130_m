package com.atguigu.chapter05.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 14:05
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092, hadoop164:9092");
        props.setProperty("group.id", "Flink03_Source_Kafka_1");
        props.setProperty("auto.reset.offset", "latest");  // 如果没有上次的位置, 则从最新的地方开始消费. 如果有上次的消费记录, 则从上次的位置开始消费
        
        //        DataStreamSource<String> s1 = env.addSource(new FlinkKafkaConsumer<String>("water_sensor", new SimpleStringSchema(), props));
        DataStreamSource<ObjectNode> s1 = env.addSource(
            new FlinkKafkaConsumer<>("water_sensor",
                                     new JSONKeyValueDeserializationSchema(false),
                                     props));
        s1.map(obj -> obj.get("value").get("name")).print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
