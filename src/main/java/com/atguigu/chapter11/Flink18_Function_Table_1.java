package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink18_Function_Table_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromValues("hello world", "hello atguigu", "abc a");
        // 内联
        /*table
            .joinLateral(call(UpperAndLength.class, $("f0")))
//            .leftOuterJoinLateral(call(UpperAndLength.class, $("f0")))
            .select($("f0"), $("upper_word"), $("len"))
            .execute()
            .print();*/
        
        // 注册后使用
        tEnv.createTemporaryFunction("ual", UpperAndLength.class);
        table
//            .joinLateral(call("ual", $("f0")))
                        .leftOuterJoinLateral(call(UpperAndLength.class, $("f0")))
            .select($("f0"), $("upper_word"), $("len"))
            .execute()
            .print();
        
        
    }
    
    @FunctionHint(output = @DataTypeHint("row<upper_word string, len int>"))
    public static class UpperAndLength extends TableFunction<Row>{
        public void eval(String s){
            if (s.length() < 10) {
                return;
            }
            
            String[] words = s.split(" ");
            for (String word : words) {
                   // 每调用一次, 得到一行数据
                collect(Row.of(word.toUpperCase(), word.length()));
            }
        }
    }
    
}

/*
hello  HELLO   5
world  WORLD   5
atguigu ATGUIGU 7
...


 */