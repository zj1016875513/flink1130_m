package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink17_Function_Scalar_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromValues("hello", "atguigu", "abc");
        
        tEnv.createTemporaryFunction("upper", ToUpperCase.class);
    
        tEnv.sqlQuery("select f0, upper(f0) from " + table).execute().print();
        
    }
    
    public static class ToUpperCase extends ScalarFunction {
        public String eval(String s) {
            return s.toUpperCase();
        }
    }
}
