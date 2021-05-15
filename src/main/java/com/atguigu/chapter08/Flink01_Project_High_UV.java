package com.atguigu.chapter08;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 10:14
 */
public class Flink01_Project_High_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> { // 对数据切割, 然后封装到POJO中
                String[] split = line.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            })
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp() * 1000)
            )
            .keyBy(UserBehavior::getBehavior)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                
                private MapState<Long, Object> userIdsState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    userIdsState = getRuntimeContext()
                        .getMapState(new MapStateDescriptor<Long, Object>("userIdsState", Long.class, Object.class));
                }
                
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    userIdsState.clear();  // 清除状态, 否则前面的窗口的数据会传递到后面的窗口
                    
                    for (UserBehavior ub : elements) {
                        userIdsState.put(ub.getUserId(), null);
                    }
                    long uv = 0L;
                    for (Long userIds : userIdsState.keys()) {
                        uv++;
                    }
                    
                    String msg = "窗口_start: " + new Date(ctx.window().getStart())
                        + ", 窗口_start: " + new Date(ctx.window().getEnd())
                        + ", uv: " + uv;
                    out.collect(msg);
                    
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
