package com.atguigu.chapter13;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/19 16:45
 */
public class Flink03_BoomFilter {
    public static void main(String[] args) throws Exception {
        
        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
            .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                @Override
                public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                    return element.getTimestamp() * 1000L;
                }
            });
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> { // 对数据切割, 然后封装到POJO中
                String[] split = line.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            })
            .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
            .assignTimestampsAndWatermarks(wms)
            .keyBy(UserBehavior::getBehavior)
            .window(TumblingEventTimeWindows.of(Time.minutes(60)))
            .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                
                private ReducingState<Long> countState;
                private ValueState<BloomFilter<Long>> bfState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    
                    bfState = getRuntimeContext()
                        .getState(new ValueStateDescriptor<BloomFilter<Long>>(
                            "bfState",
                            TypeInformation.of(new TypeHint<BloomFilter<Long>>() {})
                        ));
                    
                    countState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>(
                        "countState",
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                        Long.class
                    ));
                    
                }
                
                @Override
                public void process(String key,
                                    Context context,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
    
                    if (bfState.value() == null) {
                        BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 100 * 10000, 0.01);
                        bfState.update(bf);
                    }
                    
                    
                    for (UserBehavior ub : elements) {
                        boolean isOk = bfState.value().put(ub.getUserId());
                        if (isOk) {
                            countState.add(1L);
                        }
                        
                    }
                    
                    Long uv = countState.get();
                    
                    out.collect(context.window() + "_" + uv);
                    
                    // 清空状态
                    countState.clear();
                    bfState.clear();
                    
                }
            })
            .print();
        env.execute();
        
    }
}
