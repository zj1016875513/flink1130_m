package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink17_Function_Scalar_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromValues("hello", "atguigu", "abc");
        
        // 3. 使用
        // 3.1 在tableAPI中使用
        // 3.1.1 内联的方式使用
        table
            .select($("f0"), call(ToUpperCase.class, $("f0")).as("f0_upper"))
            .execute()
            .print();
        
        // 3.1.2 注册之后使用
        tEnv.createTemporaryFunction("upper", ToUpperCase.class);
        table
            .select($("f0"), call("upper", $("f0")).as("f0_upper"))
            .execute()
            .print();
        // 3.2 在sql中使用
        
    }
    
    public static class ToUpperCase extends ScalarFunction {
        public String eval(String s) {
            return s.toUpperCase();
        }
    }
}
