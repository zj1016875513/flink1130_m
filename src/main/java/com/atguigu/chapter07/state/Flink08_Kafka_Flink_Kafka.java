package com.atguigu.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/14 16:02
 */
public class Flink08_Kafka_Flink_Kafka {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/checkpoints/kfk"));
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);
        
        // 高级选项：
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        
        // Checkpoint 必须在20内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(20000);
        
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        Properties sourceProps = new Properties();
        sourceProps.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.setProperty("group.id", "Flink08_Kafka_Flink_Kafka");
        sourceProps.setProperty("auto.offset.reset", "latest");
        sourceProps.setProperty("isolation.level", "read_committed");  // 设置消费的时候的隔离级别
        
        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "hadoop162:9092");
        sinkProps.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");
        
        env
            .addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps))
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                    
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> t.f0 + "_" + t.f1)
            .addSink(
                new FlinkKafkaProducer<String>(
                    "s2",
                    new KafkaSerializationSchema<String>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                            return new ProducerRecord<>("s2", element.getBytes());
                        }
                    },
                    sinkProps,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                )
            );
        
        try {
            env.execute("job1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
new FlinkKafkaProducer<String>(
                    "hadoop162:9092",
                    "s2",
                    new SimpleStringSchema())
    语义不是严格一次! 不会使用二次提交
 */
