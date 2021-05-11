package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 11:24
 */
public class Flink01_Sink_Kafka_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_11", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_1", 1607527993000L, 10));
       ;
    
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092");
        // 事务的超时时间(严格一次语义使用的是kafka的事务)
        // Kafka服务器默认是15分钟, flink的默认值是1个小时
        props.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");
        env
            .fromCollection(waterSensors)
            .addSink(new FlinkKafkaProducer<WaterSensor>(
                "sensor_5",
                new KafkaSerializationSchema<WaterSensor>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                                    @Nullable Long timestamp) {
                        String jsonObj = JSON.toJSONString(element);
                        return new ProducerRecord<>(
                            "sensor_5",
                            element.getId().getBytes(),
                            jsonObj.getBytes()
    
                        );
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
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