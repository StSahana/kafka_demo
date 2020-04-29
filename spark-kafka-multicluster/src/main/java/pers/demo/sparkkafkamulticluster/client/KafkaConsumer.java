package pers.demo.sparkkafkamulticluster.client;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/*
实现了 集群1 test1->集群2 test2->集群1 test1 的数据传输。
 */
@Slf4j
@Component
public class KafkaConsumer {

    @Autowired
    private KafkaTemplate<byte[], byte[]> kafkaTemplate1;
    @Autowired
    private KafkaTemplate<byte[], byte[]> kafkaTemplate2;


    /**
     * 消费集群1 test1的数据，发往集群2 test2
     *
     * @param record
     */
    @KafkaListener(topics = "#{'${topics}'}", groupId = "#{'${groupId}'}", containerFactory = "kafkaListenerContainerFactory1")
    public void processYbrzMessage(ConsumerRecord<Integer, byte[]> record) {
        log.info("消费数据test1：{}", record.value().length);
        kafkaTemplate2.send("test2", record.value());
    }

    /**
     * 消费集群2 test2的数据，发往集群1 test1
     *
     * @param record
     */
    @KafkaListener(topics = "test2", groupId = "test2_group", containerFactory = "kafkaListenerContainerFactory2")
    public void processYbrzMessage2(ConsumerRecord<Integer, byte[]> record) {
        log.info("消费数据test2：{}", new String(record.value()));
        kafkaTemplate1.send("test1", record.value());
    }
}
