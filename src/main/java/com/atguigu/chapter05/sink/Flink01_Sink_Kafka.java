package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 11:24
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        /*DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6);
    
        s1
            .map(Object::toString)
            .addSink(new FlinkKafkaProducer<String>(
                "hadoop162:9092,hadoop163:9092",
                "t1",
                new SimpleStringSchema()
            ));*/
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        env
            .fromCollection(waterSensors)
            .map(JSON::toJSONString)
            .addSink(new FlinkKafkaProducer<String>(
                "hadoop162:9092",
                "sensor1",
                new SimpleStringSchema()  //序列化类型
            ));
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
sink的并行度如果小于kafka的分区数, 则会导致kafka的个分区数据
解决:
    1. 尽量让你sink的并行度和要写入的Kafka的topic的分区数保持一致!
    2. 写的时候最好使用轮询的方式, 或者指定分区的索引


 */