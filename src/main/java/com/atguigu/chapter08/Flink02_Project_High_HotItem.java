package com.atguigu.chapter08;

import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import com.atguigu.util.MyFlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 10:14
 */
public class Flink02_Project_High_HotItem {
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
            .keyBy(UserBehavior::getItemId)
            .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
            .aggregate(
                new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    
                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        
                        return accumulator + 1;
                    }
                    
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }
                    
                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context ctx,
                                        Iterable<Long> elements,
                                        Collector<HotItem> out) throws Exception {
                        Long count = elements.iterator().next();
                        out.collect(new HotItem(key, count, ctx.window().getEnd()));
                    }
                }
            )
            .keyBy(HotItem::getWindowEndTime)
            .process(new KeyedProcessFunction<Long, HotItem, String>() {
                
                private ValueState<Long> timerTs;
                private ListState<HotItem> hotItemState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    hotItemState = getRuntimeContext()
                        .getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    
                    timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                }
                
                @Override
                public void processElement(HotItem hotItem,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    hotItemState.add(hotItem);
                    
                    if (timerTs.value() == null) {
                        Long time = ctx.getCurrentKey() + 3000;
                        ctx.timerService().registerEventTimeTimer(time);
                        timerTs.update(time);
                    }
                    
                }
                
                @Override
                public void onTimer(long timestamp,
                                    OnTimerContext ctx,
                                    Collector<String> out) throws Exception {
                    List<HotItem> hotItems = MyFlinkUtil.iterable2List(hotItemState.get());
                    
                    // 原地排序
                    hotItems.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                    
                    StringBuilder sb = new StringBuilder();
                    sb
                        .append("-------------------\n")
                        .append("窗口结束时间: ")
                        .append(timestamp - 3000)
                        .append("\n");
                    // 取top3
                    for (int i = 0, count = Math.min(3, hotItems.size()); i < count; i++) {
                        sb.append(hotItems.get(i)).append("\n");
                    }
                    out.collect(sb.toString());
                    
                    //不清也可以, 清除是为了节省内存
                    hotItemState.clear();
                    timerTs.clear();
                    
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
