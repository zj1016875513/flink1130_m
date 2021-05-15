package com.atguigu.chapter08;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.PageCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 13:59
 */
public class Flink03_Project_High_HotPage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建WatermarkStrategy
        WatermarkStrategy<ApacheLog> wms = WatermarkStrategy
            .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(60))
            .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                @Override
                public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        
        env
            .readTextFile("input/apache.log")
            .map(line -> {
                String[] data = line.split(" ");
                SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                return new ApacheLog(data[0],
                                     df.parse(data[3]).getTime(),
                                     data[5],
                                     data[6]);
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(ApacheLog::getUrl)
            .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(20)))
            .aggregate(new AggregateFunction<ApacheLog, Long, Long>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }
                
                @Override
                public Long add(ApacheLog value, Long accumulator) {
                    return accumulator + 1L;
                }
                
                @Override
                public Long getResult(Long accumulator) {
                    return accumulator;
                }
                
                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, new ProcessWindowFunction<Long, PageCount, String, TimeWindow>() { // <url, count, endWindow>
                @Override
                public void process(String key,
                                    Context context,
                                    Iterable<Long> elements,
                                    Collector<PageCount> out) throws Exception {
                    out.collect(new PageCount(key, elements.iterator().next(), context.window().getEnd()));
                }
            })
            .keyBy(PageCount::getWindowEnd)
            .process(new KeyedProcessFunction<Long, PageCount, String>() {
                
                private ValueState<Long> timerTs;
                private ListState<PageCount> pageState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    pageState = getRuntimeContext().getListState(new ListStateDescriptor<PageCount>("pageState", PageCount.class));
                    timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                }
                
                @Override
                public void processElement(PageCount value, Context ctx, Collector<String> out) throws Exception {
                    pageState.add(value);
                    if (timerTs.value() == null) {
                        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                        timerTs.update(value.getWindowEnd());
                    }
                }
                
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    
                    // TreeSet 是一个set集合, 可以对进入这个set中的元素自动排序
                    TreeSet<PageCount> pageCounts = new TreeSet<>(new Comparator<PageCount>() {
                        @Override
                        public int compare(PageCount o1, PageCount o2) {
                            int i = o2.getCount().compareTo(o1.getCount());
                            return i == 0 ? 1 : i;
                        }
                    });
    
                    for (PageCount pageCount : pageState.get()) {
                        pageCounts.add(pageCount);
                        if(pageCounts.size() > 3){
                            pageCounts.pollLast();
                        }
                    }
                    
    
                    StringBuilder sb = new StringBuilder();
                    sb.append("窗口结束时间: " + (timestamp - 10) + "\n");
                    sb.append("---------------------------------\n");
                    for (PageCount pageCount : pageCounts) {
                        sb.append(pageCount + "\n");
                    }
                    sb.append("---------------------------------\n\n");
                    out.collect(sb.toString());
                }
                
            })
            .print();
        
        env.execute();
        
    }
}
