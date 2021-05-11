package com.atguigu.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 13:55
 */
public class Flink03_Sink_ES_1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_11", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_11", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 60));
        
        List<HttpHost> hosts = Arrays.asList(new HttpHost("hadoop162", 9200),
                                             new HttpHost("hadoop163", 9200),
                                             new HttpHost("hadoop164", 9200));
        ElasticsearchSink<WaterSensor> esSink =
            new ElasticsearchSink.Builder<WaterSensor>(
                hosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,
                                        RuntimeContext ctx,
                                        RequestIndexer indexer) {
                        IndexRequest index = Requests.indexRequest()
                            .index("sensor")
                            .type("_doc")  // 正常情况es的type不能用下划线开头. _doc 是唯一个的待下划线的type
                            .id(element.getId())
                            .source(JSON.toJSONString(element), XContentType.JSON);
                        indexer.add(index);
                    }
                }
            )
                .build();
        
        env
            .fromCollection(waterSensors)
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(esSink);
        
        env.execute();
    }
}
