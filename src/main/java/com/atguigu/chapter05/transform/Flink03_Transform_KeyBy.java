package com.atguigu.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 13:58
 */
public class Flink03_Transform_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 100, 200, 400);
        s1
            .keyBy(new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value % 2 == 0 ? 0 : 1;  // 0 % 2 = 0   1%2=1
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
/*
1. 元素的key一样一定会进入到同一个并行度   √
5. 如果key不一样, 一定不会进入同一个并行度 ×


keyBy确实是按照key的哈希值来选择并行度, 是使用了双重hash



1 << 7 = 128
0000 0001   1
0000 0010   2
0000 0100   4
new KeyGroupStreamPartitioner<>(keySelector, StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)


0/1, 128, 2
KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);

128, 2, [0-127]
computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));

        0/1, 128
        computeKeyGroupForKeyHash(key.hashCode(), maxParallelism)

            int值%128
            MathUtils.murmurHash(keyHash) % maxParallelism;


[0-127]*2/128 =
keyGroupId * parallelism / maxParallelism;
如果keyGroupId小于64, 最终结果是0
如果keyGroupId大于等于64 最终结果是1



 */