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
                    System.out.println("xxxx....");  //有几个并行度就输出几次
                    sumState = getRuntimeContext() // 监控状态
                        .getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
                }

                @Override
                public void processElement(Integer value,
                                           Context ctx,
                                           Collector<Integer> out) throws Exception {
                    Integer sum = sumState.value();

//                    System.out.println("sum="+sum+" value="+value);

                    sum = sum == null ? 0 : sum;
                    sum += value;

//                    System.out.println("sum=" + sum+"\n");

                    out.collect(sum);
                    sumState.update(sum);

                }
            })
            .print();
        //  xxxx....   因为3个并行度 所以输出3个xxxx
        //  xxxx....
        //  xxxx....
        //  3> 1       因为keyby之后会分为两组，奇数分一组，偶数分一组， 奇数组的sum为  1 4 9 16  偶数组的sum为 2 6 12
        //  3> 2
        //  3> 4
        //  3> 6
        //  3> 9
        //  3> 12
        //  3> 16

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
