package com.atguigu.chapter10;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/17 15:24
 */
public class Flink02_CEP_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
            .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                @Override
                public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        KeyedStream<OrderEvent, Long> stream = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] datas = line.split(",");
                return new OrderEvent(
                    Long.valueOf(datas[0]),
                    datas[1],
                    datas[2],
                    Long.parseLong(datas[3]) * 1000);
            
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(OrderEvent::getOrderId);
        
        // 需求: 找到异常订单   有pay有create但是超时,  有pay没有create, 或者有create没有pay
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
            .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "create".equals(value.getEventType());
                }
            }).optional()
            .next("pay")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "pay".equals(value.getEventType());
                }
            })
            .within(Time.minutes(45));
    
        PatternStream<OrderEvent> ps = CEP.pattern(stream, pattern);
    
        SingleOutputStreamOperator<String> mail = ps.select(
            new OutputTag<String>("late") {},
            new PatternTimeoutFunction<OrderEvent, String>() {
                @Override
                public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                    return pattern.toString();
                }
            },
            new PatternSelectFunction<OrderEvent, String>() {
                @Override
                public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                    Set<String> keys = pattern.keySet();
                    if (!keys.contains("create") ) {
                        System.out.println(keys);
                        return "异常: " + pattern.get("pay").toString();
                    }
                    return "";
                }
                
            }
        );
        mail.filter(msg -> msg.contains("异常")).print("有pay没create订单");
        mail.getSideOutput(new OutputTag<String>("late") {}).print("超时订单");
    
        env.execute();
    }
}
