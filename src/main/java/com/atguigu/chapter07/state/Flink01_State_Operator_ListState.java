package com.atguigu.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 15:56
 */
public class Flink01_State_Operator_ListState {
    public static void main(String[] args) {
        // 把每个单词存入到我们的列表状态
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(3000);
    
        env
            .socketTextStream("hadoop162", 9999)
            .flatMap(new MyFlatMapFunction())
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyFlatMapFunction implements FlatMapFunction<String, String>, CheckpointedFunction {
    
        private ListState<String> listState;
        ArrayList<String> list = new ArrayList<>();
        
    
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            
            for (String word : value.split(",")) {
                list.add(word);
                out.collect(list.toString());
            }
            
            
        }
        
        // 做Checkpoint, 其实把状态持久化存储, 将来恢复的可以从快照中恢复状态
        // 周期的执行: 需要开启Checkpoint
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
//            System.out.println("snapshotState...");
            // 当做快照的时候, 把需要保存的数据存储到状态中
            listState.update(list); /// 用list中的元素去覆盖列表状态
        }
    
        // 初始化状态:  在这里应该把状态恢复到停机之前
        // 这个将来是在程序启动的时候执行, 或者重启的执行
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
//            System.out.println("initializeState...");
    
//            listState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<String>("listState", String.class));
            listState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("listState", String.class));
            
            // 把状态中的数据, 存入到java的list集合中
            for (String w : listState.get()) {
                list.add(w);
            }
            
        }
    }
}
/*
1. 列表状态
2. 联合列表
3. 广播状态

 */
