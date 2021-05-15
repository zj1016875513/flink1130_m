package com.atguigu.chapter08;

import com.atguigu.bean.LoginEvent;
import com.atguigu.util.MyFlinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/15 15:37
 */
public class Flink05_Project_High_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<LoginEvent> wms = WatermarkStrategy
            .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                @Override
                public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                    return element.getEventTime();
                }
            });
        env
            .readTextFile("input/LoginLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new LoginEvent(Long.valueOf(data[0]),
                                      data[1],
                                      data[2],
                                      Long.parseLong(data[3]) * 1000L);
            })
            .assignTimestampsAndWatermarks(wms)
            // 按照用户id分组
            .keyBy(LoginEvent::getUserId)
            .process(new KeyedProcessFunction<Long, LoginEvent, String>() {
    
                private ListState<Long> failTsState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    failTsState = getRuntimeContext()
                        .getListState(new ListStateDescriptor<Long>("failTsState", Long.class));
                }
    
                @Override
                public void processElement(LoginEvent value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    /*
                    有集合, 只存储两个时间戳: 登录失败的时间戳
                    
                    然后去判断这个两个时间戳是否相差2s以内入是,在任务这个用户在恶意登录
                    
                    
                     */
                    if("fail".equals(value.getEventType())){
                        failTsState.add(value.getEventTime());
    
                        List<Long> list = MyFlinkUtil.iterable2List(failTsState.get());
                        list.sort(Long::compareTo);
                        if(list.size() == 2) { // 碰到两个失败
                            long delta = (list.get(1) - list.get(0)) / 1000;
                            if (delta <= 2) {
                                out.collect(ctx.getCurrentKey() + " 在恶意登录....");
                            }
                            list.remove(0);
                            
                            // 更新状态
                            failTsState.update(list);
                        }
    
                    }else{ // 成功
                        failTsState.clear();
                    }
                }
            })
            .print();
    
        env.execute();
        
    
    }
}
