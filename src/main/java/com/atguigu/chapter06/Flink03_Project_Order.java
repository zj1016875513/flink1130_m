package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 9:48
 */
public class Flink03_Project_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // 获取两个流
        // 1. 订单流
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] datas = line.split(",");
                return new OrderEvent(
                    Long.valueOf(datas[0]),
                    datas[1],
                    datas[2],
                    Long.valueOf(datas[3]));
                
            })
            .filter(event -> "pay".equals(event.getEventType()));
        // 2. 交易流
        SingleOutputStreamOperator<TxEvent> txDS = env
            .readTextFile("input/ReceiptLog.csv")
            .map(line -> {
                String[] datas = line.split(",");
                return new TxEvent(datas[0], datas[1], Long.valueOf(datas[2]));
            });
        
        // 3. 连接两个流
        orderEventDS
            .connect(txDS)
            .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
            .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                Map<String, OrderEvent> orderEventMap = new HashMap<>();
                Map<String, TxEvent> txEventMap = new HashMap<>();
                
                @Override
                public void processElement1(OrderEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    // 订单数据
                    // 先去对方的集合中查看有没有对应的交易信息
                    TxEvent txEvent = txEventMap.get(value.getTxId());
                    if (txEvent != null) {
                        // 交易信息已经到, 对账成功
                        out.collect("订单: " + value.getOrderId() + " 对账成功...");
                    } else {
                        // 交易还没有来, 把自己存储到属于自己的集合中
                        orderEventMap.put(value.getTxId(), value);
                    }
                    
                }
                
                @Override
                public void processElement2(TxEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    // 交易数据
                    OrderEvent orderEvent = orderEventMap.get(value.getTxId());
                    if (orderEvent != null) {
                        out.collect("订单: " + orderEvent.getOrderId() + " 对账成功...");
                    } else {
                        txEventMap.put(value.getTxId(), value);
                    }
                    
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
