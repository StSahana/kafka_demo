package pers.demo.springkafkaautoconfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
@SpringBootApplication
public class SpringKafkaAutoconfigApplication {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaAutoconfigApplication.class, args);
    }

    //监听集群2上的test2 topic的消息，发送到集群1上的test1 topic
    @KafkaListener(topics = "#{'${topics}'}", groupId = "#{'${groupId}'}")
    public void processYbrzMessage(ConsumerRecord<byte[], byte[]> record) {
        log.info("消费数据test1：{}", new String(record.value()));
//        kafkaTemplate.send("test1",record.value());
    }



}
