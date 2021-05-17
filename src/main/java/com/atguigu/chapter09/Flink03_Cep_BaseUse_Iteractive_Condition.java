package com.atguigu.chapter09;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/17 9:48
 */
public class Flink03_Cep_BaseUse_Iteractive_Condition {
    public static void main(String[] args) {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 1. 先获取数据流
        SingleOutputStreamOperator<WaterSensor> stream = env
            .readTextFile("input/sensor.json")
            .map(line -> JSON.parseObject(line, WaterSensor.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            );
        
        // 2. 写规则: 定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
            .<WaterSensor>begin("start")
            .where(new IterativeCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value,
                                      Context<WaterSensor> ctx) throws Exception {
                    Iterable<WaterSensor> start = ctx.getEventsForPattern("start");
                    for (WaterSensor waterSensor : start) {
                        System.out.println("x: " + waterSensor);
                    }
                    return "sensor_1".equals(value.getId());
                }
            })
            .times(2);
           
        
        // 3. 使用模式去匹配数据类, 把模式作用在数据流中
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        
        // 4. 得到符合条件的数据
        ps
            .select(new PatternSelectFunction<WaterSensor, String>() {
                @Override
                public String select(Map<String, List<WaterSensor>> map) throws Exception {
                    return map.toString();
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
