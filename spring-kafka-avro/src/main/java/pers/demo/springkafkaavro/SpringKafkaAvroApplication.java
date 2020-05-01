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
    Schema schema;
    AtomicLong atomicLong=new AtomicLong(0);

    /**
     * 在spring 装载主类时将schema初始化
     *
     * @param baseUrl
     * @param TOPIC
     */
    SpringKafkaAvroApplication(@Value("${spring.kafka.consumer.properties.schema.registry.url}") String baseUrl,
                               @Value("${topics}") String TOPIC) {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(baseUrl, 100);
        String stringSchema = null;
        try {
            stringSchema = client.getLatestSchemaMetadata(TOPIC).getSchema();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        this.schema = new Schema.Parser().parse(stringSchema);
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaAvroApplication.class, args);
    }

    /**
     * 订阅topic并打印
     *
     * @param record
     */
    @KafkaListener(topics = "#{'${topics}'}", concurrency = "1")
    private void processMessage(ConsumerRecord<String, byte[]> record) {
        byte[] arr = record.value();
        JSONArray jsonArray = new AvroUtil().byte2Array(arr, schema);
        log.info("batch size:{}",jsonArray.size());
        log.info("total size:{}",atomicLong.addAndGet(jsonArray.size()));
    }


}
