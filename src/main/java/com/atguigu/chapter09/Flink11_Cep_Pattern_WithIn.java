package com.atguigu.chapter09;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/17 9:48
 */
public class Flink11_Cep_Pattern_WithIn {
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
        
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
            .<WaterSensor>begin("start")
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_1".equals(value.getId());
                }
            })
            .next("end")
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return "sensor_2".equals(value.getId());
                }
            })
            .within(Time.seconds(3));
        
        // 3. 使用模式去匹配数据类, 把模式作用在数据流中
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);
        
        // 4. 得到符合条件的数据
        SingleOutputStreamOperator<String> select = ps
            .select(
                new OutputTag<String>("late") {},
                new PatternTimeoutFunction<WaterSensor, String>() {
                    @Override
                    public String timeout(Map<String, List<WaterSensor>> map,
                                          long timeoutTimestamp) throws Exception {
//                        return map.toString(); //可以发现超时的只显示start里的内容，没有end的内容
                        return map.get("start").get(0).toString(); //获取"start"内的内容，再获取数组中第一个json对象，再tostring；
                    }
                },
                new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString(); //既显示start的内容，也显示end里的内容
                    }
                }
            );
        select.print("选中的");
        select.getSideOutput(new OutputTag<String>("late") {}).print("超时的");
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
