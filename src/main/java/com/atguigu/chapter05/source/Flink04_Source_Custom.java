package com.atguigu.chapter05.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 14:34
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) {
        // 从socket读数据的source
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        DataStreamSource<WaterSensor> s1 = env.addSource(new MySocketSource("hadoop162", 9999));
        s1.print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MySocketSource implements SourceFunction<WaterSensor> {
        
        private String host;
        private int port;
        private boolean isCancel = false;
        
        public MySocketSource(String host, int port) {
            this.host = host;
            this.port = port;
        }
        
        // 从socket读取数据
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Socket socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            String line = reader.readLine();
            // sensor_1,1,10
            while (!isCancel && line != null) {
                String[] data = line.split(",");
                ctx.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
                line = reader.readLine();  // 阻塞式
            }
            
        }
        
        // 用来关闭source, 停止读数据
        // 不会自动调用, 需要手动在需要的时候调用
        @Override
        public void cancel() {
            isCancel = true;
        }
    }
}
