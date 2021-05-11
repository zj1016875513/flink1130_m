package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 13:55
 */
public class Flink02_Sink_Redis_2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_11", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 60));
        
        FlinkJedisPoolConfig jedisConif = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop162")
            .setPort(6379)
            .setTimeout(10 * 1000)
            .setMaxTotal(100)
            .setMaxIdle(10)
            .setMinIdle(2)
            .build();
        /*
        1. 写字符串
            key             value
            sensor_11       json格式的字符串
         */
        env
            .fromCollection(waterSensors)
            .addSink(new RedisSink<>(jedisConif, new RedisMapper<WaterSensor>() {
                // string list set hash zset
                // set   lpush rpush  sadd  hset ...
                @Override
                public RedisCommandDescription getCommandDescription() {
                    // 参数1: 表示这次写数据的使用的命令(由要写入的数据的类型来决定)
                    // 参数2: 只对hash和zset有效, 其他类型无效
                    return new RedisCommandDescription(RedisCommand.RPUSH, "random");
                }
    
                // 从数据中提取一个key 不是hash和zset的时候, 就是表示key
                // ?
                @Override
                public String getKeyFromData(WaterSensor data) {
                    return data.getId();
                }
    
                // 具体的值
                @Override
                public String getValueFromData(WaterSensor data) {
                    return JSON.toJSONString(data);
                }
            }));
        
        env.execute();
    }
}
