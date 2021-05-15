package com.atguigu.chapter08;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 16:25
 */
public class Flink06_Project_High_Order {
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
        env
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
            .keyBy(OrderEvent::getOrderId)
            .process(new KeyedProcessFunction<Long, OrderEvent, String>() {
                
                private ValueState<OrderEvent> createState;
                private ValueState<OrderEvent> payState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class));
                }
                
                @Override
                public void processElement(OrderEvent value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 第一个create或者pay来的时候, 应该定义一个定时器, 比如让他60分钟后执行
                    // 如果另外一个也来, 则取消定时器
                    // 如果定时器按时被触发, 可以另外一个没有来
                    if (createState.value() == null && payState.value() == null) { // 这个订单的第一条数据来了
                        ctx.timerService().registerEventTimeTimer(value.getEventTime() + 60 * 60 * 1000);
                    } else { // 第二个来的时候取消定时器
                        // 取消定时器
                        long timerTs = (createState.value() == null
                            ? payState.value().getEventTime()
                            : createState.value().getEventTime())
                            + 60 * 60 * 1000;
                        ctx.timerService().deleteEventTimeTimer(timerTs);
                    }
                    
                    String eventType = value.getEventType();
                    if ("create".equals(eventType)) {
                        if (payState.value() == null) {  // pay还没有来
                            // 把create的数据存入到状态, 为将来pay紧邻2之后做匹配
                            createState.update(value);
                        } else if (payState.value().getEventTime() - value.getEventTime() <= 45 * 60 * 1000) {
                            // 这个订单正常
                            out.collect(ctx.getCurrentKey() + " 正常创建和支付");
                        } else {
                            // 这个订单被超时支付, 检查系统是否有漏洞
                            out.collect(ctx.getCurrentKey() + " 被超时支付, 请检查系统是否有漏洞");
                        }
                    } else { // pay
                        if (createState.value() == null) {
                            payState.update(value);
                        } else if (value.getEventTime() - createState.value().getEventTime() <= 45 * 60 * 1000) {
                            out.collect(ctx.getCurrentKey() + " 正常创建和支付");
                        } else {
                            out.collect(ctx.getCurrentKey() + " 被超时支付, 请检查系统是否有漏洞");
                        }
                    }
                }
                
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    if (createState.value() == null) {
                        out.collect(payState.value().getOrderId() + " 异常, 有支付, 但是没有下单, 系统的漏洞");
                    } else {
                        out.collect(createState.value().getOrderId() + " 异常, 用户没钱, 没有下单");
                    }
                }
            })
            .print();
        
        env.execute();
    }
}
