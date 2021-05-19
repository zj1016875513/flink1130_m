package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 9:11
 */
public class Flink18_Function_Table_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromValues("hello world", "hello atguigu", "abc a");
        tEnv.createTemporaryView("words", table);
        
        // 注册后使用
        tEnv.createTemporaryFunction("ual", UpperAndLength.class);
    
        /*tEnv.sqlQuery("select f0, upper_word, len " +
                          "from words " +
                          "join lateral table(ual(f0)) on 1=1").execute().print();
       */
    
        /*tEnv.sqlQuery("select f0, upper_word, len " +
                          "from words," +
                          "lateral table(ual(f0))").execute().print();*/
    
        tEnv.sqlQuery("select f0, upper_word, len " +
                          "from words " +
                          "left join lateral table(ual(f0)) on true").execute().print();
    
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