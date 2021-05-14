package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */
public class Flink07_State_Backend {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");  // shift + ctrl + u
        // 把每个单词存入到我们的列表状态
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        // 默认就是MemoryStateBackend, 仅仅管理本地. 如果要管理远程, 需要开启Checkpoint
        //env.setStateBackend(new MemoryStateBackend());
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/flink/ck"));
//        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/flink/ck1"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/flink/ck1", true));
        env.enableCheckpointing(3000);
        
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]));
                
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private ValueState<Integer> lastVcState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 监控状态由flink的运行时对象管理
                    lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer lastVc = lastVcState.value();
                    Integer currentVc = value.getVc();
                    if (lastVc != null) {
                        if (currentVc - lastVc > 10) {
                            out.collect(ctx.getCurrentKey() + " 水位上升: " + (currentVc - lastVc) + " 红色预警");
                        }
                    }
                    lastVcState.update(currentVc);
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
