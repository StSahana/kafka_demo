package pers.demo.springkafkaavro;

import com.alibaba.fastjson.JSONArray;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import pers.demo.springkafkaavro.util.AvroUtil;
import pers.demo.springkafkaavro.util.SchemaUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * spring kafka + avro
 * 单节点最高处理500M/s的流量（服务器40核silver，万兆光纤）
 * 如果是kafka0.9，记得切换springboot版本为2.1
 * 见：https://spring.io/projects/spring-kafka
 */
@Slf4j
@SpringBootApplication
public class SpringKafkaAvroApplication {
    @Autowired
    AvroUtil avroUtil;
    @Autowired
    KafkaTemplate<String, byte[]> kafkaTemplate;
    @Autowired
    SchemaUtil schemaUtil;
    AtomicLong atomicLong=new AtomicLong(0);

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaAvroApplication.class, args);
    }

    /**
     * 订阅topic并打印
     *
     * @param record
     */
    @KafkaListener(topics = "#{'${topics}'.split(',')}", concurrency = "#{'${concurrency}'}")
    private void processMessage(ConsumerRecord<String, byte[]> record) {
        byte[] arr = record.value();
        JSONArray jsonArray = new AvroUtil().byte2Array(arr, schemaUtil.getSchema(record.topic()));
        log.debug("first record:{}",jsonArray.get(0));
        log.info("batch size:{}",jsonArray.size());
        log.info("total size:{}",atomicLong.addAndGet(jsonArray.size()));
    }


}
