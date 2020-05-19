package pers.demo.springkafkaavro;

import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import pers.demo.springkafkaavro.util.AvroDeserializerUtil;
import pers.demo.springkafkaavro.util.AvroSerializerUtil;
import pers.demo.springkafkaavro.util.SchemaUtil;

import java.util.Arrays;
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
    AvroDeserializerUtil avroUtil;
//    @Autowired
//   KafkaTemplate<String, byte[]> kafkaTemplate;
    @Autowired
    SchemaUtil schemaUtil;
    @Value("${statistic.enable}")
    private  boolean enableStatistic;
    @Value("${print.all.enable}")
    private boolean enablePrintAll;
    @Value("${print.first.enable}")
    private boolean enablePrintFirst;
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
        JSONArray jsonArray = new AvroDeserializerUtil().byte2Array(arr, schemaUtil.getSchema(record.topic()));
        if (enablePrintAll)
            log.info("{}=>all record:{}",record.topic(),jsonArray);
        if(enablePrintFirst)
            log.info("{} first value:{}",record.topic(),jsonArray.get(0));
//        byte[] arr2=new AvroSerializerUtil().array2Byte(schemaUtil.getSchema(record.topic()),jsonArray);
//       JSONArray jsonArray2 = new AvroDeserializerUtil().byte2Array(arr2, schemaUtil.getSchema(record.topic()));
        if (enableStatistic){
            log.info("{}=>batch size:{}",record.topic(),jsonArray.size());
            log.info("{}=>total size:{}",record.topic(),atomicLong.addAndGet(jsonArray.size()));
        }

    }


}
