package com.atguigu.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */
public class Flink01_State_Operator_BroadState {
    public static void main(String[] args) {
        // 把每个单词存入到我们的列表状态
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(3000);
        MapStateDescriptor<String, String> bdStateDescriptor = new MapStateDescriptor<>("bdState", String.class, String.class);
        DataStreamSource<String> dataStream = env
            .socketTextStream("hadoop162", 8888);
        
        BroadcastStream<String> bdStream = env
            .socketTextStream("hadoop162", 9999)
            .broadcast(bdStateDescriptor);
        
        dataStream
            .connect(bdStream)
            .process(new BroadcastProcessFunction<String, String, String>() {
                //数据流的数据会走这里
                // 获取广播状态
                @Override
                public void processElement(String data,
                                           ReadOnlyContext ctx,
                                           Collector<String> out) throws Exception {
                    ReadOnlyBroadcastState<String, String> dbState = ctx.getBroadcastState(bdStateDescriptor);
                    String logic = dbState.get("switch");
                    if ("0".equals(logic)) {
                        out.collect("切换到 0 号处理逻辑");
                    } else if ("1".equals(logic)) {
                        out.collect("切换到 1 号处理逻辑");
                    } else {
                        out.collect("切换到 默认 处理逻辑");
                    }
                }
                
                // 广播流的数据会走这里
                // 把广播流的数据, 我会放入到广播状态中
                @Override
                public void processBroadcastElement(String value,
                                                    Context ctx,
                                                    Collector<String> out) throws Exception {
                    BroadcastState<String, String> bdState = ctx.getBroadcastState(bdStateDescriptor);
                    bdState.put("switch", value);
                    
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
/*
业务数据有多重处理逻辑, 实际工作的时候, 只会选一种. 通过动态的方式去更改处理逻辑

广播流中广播一个值, 数据流中收到这个值之后, 选择相应的处理逻辑



广播状态的使用:
  需要两个流
    数据流
        正常的数据, 处理业务逻辑, 需要一些状态, 而且每个并行度读取到的状态应该完全一样, 用到广播状态
    
    广播流
        广播流中的数据, 会被作为广播状态, 广播给数据流中的每个并行度
 */
