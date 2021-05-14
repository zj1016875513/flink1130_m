package com.atguigu.chapter07.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.util.MyFlinkUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */
public class Flink03_State_Keyed_List {
    public static void main(String[] args) {
        // 把每个单词存入到我们的列表状态
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
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
                
                private ListState<Integer> vcListState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    vcListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState", Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer vc = value.getVc();
                    vcListState.add(vc);
                    
                    List<Integer> list = MyFlinkUtil.iterable2List(vcListState.get());
    
//                    list.sort((o1, o2) -> o2.compareTo(o1));
                    list.sort(Comparator.reverseOrder()); // 排序是原地排序
                    
                    if(list.size() > 3){
                        list.remove(3);
                    }
    
                    // 更新列表状态(覆盖)
                    vcListState.update(list);
    
                    out.collect(ctx.getCurrentKey() + ": "+list.toString());
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
