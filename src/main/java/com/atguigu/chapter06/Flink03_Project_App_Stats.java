package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 9:48
 */
public class Flink03_Project_App_Stats {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        env
            .addSource(new MarketSource())
            // 行为和渠道 : 1. 拼成元组  2. 拼成一个字符串 install_xiaomi
            .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                    return Tuple2.of(value.getBehavior() + "_" + value.getChannel(), 1L);
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public static class MarketSource implements SourceFunction<MarketingUserBehavior> {
        
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            String[] behaviors = {"install", "uninstall", "update", "download"};
            String[] channels = {"huawei", "apple", "xiaomi", "oppo", "vivo"};
            while (true) {
                Long userId = (long) (random.nextInt(2000) + 1);
                String behavior = behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                Long timestamp = System.currentTimeMillis();
                
                ctx.collect(new MarketingUserBehavior(userId, behavior, channel, timestamp));
                Thread.sleep(500);
            }
        }
        
        @Override
        public void cancel() {
        
        }
    }
}
