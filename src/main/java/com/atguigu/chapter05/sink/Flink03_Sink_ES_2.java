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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/11 13:55
 */
public class Flink03_Sink_ES_2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
       
        List<HttpHost> hosts = Arrays.asList(new HttpHost("hadoop162", 9200),
                                             new HttpHost("hadoop163", 9200),
                                             new HttpHost("hadoop164", 9200));
    
        ElasticsearchSink.Builder<WaterSensor> esSinkBuilder = new ElasticsearchSink.Builder<>(
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
        );
        Class<ElasticsearchSink.Builder> c = ElasticsearchSink.Builder.class;
        Field bulkRequestsConfig = c.getDeclaredField("bulkRequestsConfig");
        bulkRequestsConfig.setAccessible(true);
        Map<String, String> o = (Map<String, String>) bulkRequestsConfig.get(esSinkBuilder);
        System.out.println(o);
        System.out.println(o.get("bulk.flush.max.size.mb"));
        System.out.println(o.get("bulk.flush.interval.ms"));
    
        esSinkBuilder.setBulkFlushMaxActions(1);
        
//        esSinkBuilder.setBulkFlushMaxSizeMb();
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(esSinkBuilder.build());
        
        env.execute();
    }
}
