package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 13:55
 */
public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(
                JdbcSink
                    .<WaterSensor>sink(
                        "replace into sensor values (?, ?, ?)",
                        (ps, value) -> {
                            ps.setString(1, value.getId());
                            ps.setLong(2, value.getTs());
                            ps.setInt(3, value.getVc());
                        },
                        JdbcExecutionOptions.builder()
                            .withBatchSize(10)
                            .withBatchIntervalMs(200)
                            .withMaxRetries(5)
                            .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://hadoop162:3306/test")
                            .withDriverName("com.mysql.jdbc.Driver")
                            .withUsername("root")
                            .withPassword("aaaaaa")
                            .build()
                    )
            );
        
        env.execute();
    }
    
}
