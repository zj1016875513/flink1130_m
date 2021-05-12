package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 16:29
 */
public class Flink02_Project_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        
        env
            .readTextFile("input/UserBehavior.csv")
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    String[] data = value.split(",");
                    UserBehavior ub = new UserBehavior(
                        Long.valueOf(data[0]),
                        Long.valueOf(data[1]),
                        Integer.valueOf(data[2]),
                        data[3],
                        Long.valueOf(data[4]));
                    if ("pv".equals(ub.getBehavior())) {
                        out.collect(    Tuple2.of("pv", ub.getUserId())    );
                    }
                    
                }
            })
            .keyBy(t -> t.f0)
            .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                HashSet<Long> uids = new HashSet<>();
                
                @Override
                public void processElement(Tuple2<String, Long> value,
                                           Context ctx,
                                           Collector<Long> out) throws Exception {
                    
                    // 有一条pv数据, 则把uid存起来, 然后计算uid的个数, 就是uv
                    /*int preSize = uids.size();
                    uids.add(value.f1);
                    int postSize = uids.size();
                    if (postSize > preSize) {
                        
                        out.collect((long) uids.size());
                    }*/
    
                    if (uids.add(value.f1)) {
                        out.collect((long) uids.size());
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
