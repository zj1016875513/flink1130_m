package com.atguigu.chapter08;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 14:25
 */
public class Flink04_Project_High_BlackList {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<AdsClickLog> wms = WatermarkStrategy
            .<AdsClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
                @Override
                public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                    return element.getTimestamp() * 1000L;
                }
            });
        
        SingleOutputStreamOperator<String> result = env
            .readTextFile("input/AdClickLog.csv")
            .map(line -> {
                String[] datas = line.split(",");
                return new AdsClickLog(Long.valueOf(datas[0]),
                                       Long.valueOf(datas[1]),
                                       datas[2],
                                       datas[3],
                                       Long.valueOf(datas[4]));
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(ads -> ads.getUserId() + "_" + ads.getAdsId())
            .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
                
                private ValueState<String> dateState;
                private ValueState<Boolean> warnState;
                private ReducingState<Long> clickCountState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    clickCountState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>(
                        "clickCountState",
                        Long::sum,
                        Long.class));
                    
                    warnState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("warnState", Boolean.class));
                    
                    dateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dateState", String.class));
                }
                
                @Override
                public void processElement(AdsClickLog log,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 状态清除:  如果 dateState是空值,表示是我们的程序第一次启动  . 后面的天, 第一条来的时候
                    // 状态不为空, 但是日期和第一条数据不一样
                    String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date(log.getTimestamp() * 1000));
                    System.out.println(today);
                    String lastDate = dateState.value();
                    
                    if (lastDate == null || !lastDate.equals(today)) {
                        clickCountState.clear();
                        warnState.clear();
                        dateState.clear();
                        dateState.update(today);
                    }
                    
                    // 一旦某个用户的对某个广告的点击量超过了100,
                    // 则把这个用户放入黑名单(放入侧输出流), 以后就不再处理是个用户对这个广播的点击数据
                    
                    clickCountState.add(1L);
                    Long clickCount = clickCountState.get();
                    if (clickCount > 100) {
                        if (warnState.value() == null) {
                            // 写入到侧输出流
                            String msg = ctx.getCurrentKey() + " 点击量是: " + clickCount + " 发出预警";
                            ctx.output(new OutputTag<String>("alert") {}, msg);
                            
                            warnState.update(true);
                        }
                        
                    } else {
                        String msg = ctx.getCurrentKey() + " 点击量是: " + clickCount;
                        out.collect(msg);
                    }
                }
            });
        
        result.print("正常");
        result.getSideOutput(new OutputTag<String>("alert") {}).print("黑名单数据");
        
        env.execute();
        
    }
}
