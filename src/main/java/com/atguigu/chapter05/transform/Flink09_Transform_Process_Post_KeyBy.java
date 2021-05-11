package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink09_Transform_Process_Post_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        
        s1
            .keyBy(x -> x % 2)
            
            .process(new KeyedProcessFunction<Integer, Integer, Integer>() {
                
                private ValueState<Integer> sumState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    System.out.println("xxxx....");
                    sumState = getRuntimeContext() // 监控状态
                        .getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
                }
                
                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    Integer sum = sumState.value();
                    sum = sum == null ? 0 : sum;
                    sum += value;
                    
                    out.collect(sum);
                    sumState.update(sum);
                    
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
