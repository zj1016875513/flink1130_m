package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 13:55
 */
public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(new MysqlSink());
        
        env.execute();
    }
    
    public static class MysqlSink extends RichSinkFunction<WaterSensor> {
        
        private Connection conn;  // ctrl+alt+f
        private PreparedStatement ps;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            //建立到mysql的连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test", "root", "aaaaaa");
//            String sql = "insert into sensor values(?, ?, ?)";
//            String sql = "insert into sensor values(?, ?, ?) on duplicate key update vc=?";  // mysql的幂等性
            String sql = "replace into sensor values(?, ?, ?)";  // mysql的幂等性
            ps = conn.prepareStatement(sql);
        }
        
        @Override
        public void invoke(WaterSensor value,
                           Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
//            ps.setInt(4, value.getVc());
            ps.execute();
        }
        
        @Override
        public void close() throws Exception {
            
            if (ps != null) {
                ps.close();
            }
            // 关闭连接
            if (conn != null) {
                conn.close();
            }
            
        }
    }
}
