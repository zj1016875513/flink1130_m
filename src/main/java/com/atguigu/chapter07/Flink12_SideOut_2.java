package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 16:39
 */
public class Flink12_SideOut_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);
        
        // sensor_1进入主流
        // sensor_2 进入一个侧输出流
        // sensor_3 及其他所有id都进入另外一个侧输出流
        SingleOutputStreamOperator<WaterSensor> main = env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));
                
            })
            .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<WaterSensor> out) throws Exception {
                    if ("sensor_1".equals(value.getId())) {
                        out.collect(value);
                    } else if ("sensor_2".equals(value.getId())) {
                        ctx.output(new OutputTag<WaterSensor>("s2") {}, value);
                    } else {
                        ctx.output(new OutputTag<WaterSensor>("s3") {}, value);
                    }
                }
            });
        
        main.print("s1");
        main.getSideOutput(new OutputTag<WaterSensor>("s2") {}).print("s2");
        main.getSideOutput(new OutputTag<WaterSensor>("s3") {}).print("s3");
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
