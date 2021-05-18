package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/18 11:06
 */
public class Flink05_Table_Sink_File {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStream<WaterSensor> waterSensorStream =
            env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                             new WaterSensor("sensor_1", 2000L, 20),
                             new WaterSensor("sensor_2", 3000L, 30),
                             new WaterSensor("sensor_1", 4000L, 40),
                             new WaterSensor("sensor_1", 5000L, 50),
                             new WaterSensor("sensor_2", 6000L, 60))
                .map(x -> x);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        Table sensorTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
            .where($("id").isEqual("sensor_1"))
            .select($("id"), $("ts"), $("vc"));
        
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        // 在内存中创建一个临时表, 与文件中的文件进行关联, 如果向这个表插入数据, 则数据会直接进入关联文件
        tableEnv
            .connect(new FileSystem().path("input/1130"))
            .withFormat(new Csv())
//            .withFormat(new Json())  // 不支持json
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        resultTable.executeInsert("sensor");
        
    }
}
