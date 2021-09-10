package com.atguigu.chapter07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/12 16:39
 */
public class Flink11_Window_WaterMark_Custom_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);//将水印发射时间由200ms一次改为1000ms一次
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));
                
            })
            .assignTimestampsAndWatermarks(
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPeriodWaterMark(); //自定义水印策略
                    }
                }
                    .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                        @Override
                        public long extractTimestamp(WaterSensor ws, long ts) {
                            return ws.getTs() * 1000;
                        }
                    })
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    int count = 0;
                    for (WaterSensor ws : elements) {
                        count++;
                    }
                    TimeWindow w = ctx.window();
                    out.collect(
                        "当前Key=" + key
                            + "窗口: [ " + w.getStart() / 1000 + "," + w.getEnd() / 1000 + " ), "
                            + "元素个数: " + count
                    );
                }
            })
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //自定义水印发生器
    public static class MyPeriodWaterMark implements WatermarkGenerator<WaterSensor> {
        public MyPeriodWaterMark(){
            System.out.println("xxxxxx");
        }
        
        long maxTs = Long.MIN_VALUE + 3000 + 1;


        //计算最大时间戳，保证时间不倒流
        //每来一条数据就执行一次方法
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyPeriodWaterMark.onEvent...");
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - 3000 - 1)); //这个方法在这里发射水印就是间歇性水印
        }


        //周期200ms调用一次，然后向流中放入水印
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyPeriodWaterMark.onPeriodicEmit....");
            //output.emitWatermark(new Watermark(maxTs - 3000 - 1)); //这个方法在这里发射水印就是周期性水印
        }
    }
}
