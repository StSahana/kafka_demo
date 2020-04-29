package pers.demo.sparkkafkamulticluster.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.security.jaas.KafkaJaasLoginModuleInitializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@EnableKafka
public class KafkaContainer {
    @Value("${bootstrap.servers1}")
    private String BrokerServers1;

    @Value("${bootstrap.servers2}")
    private String BrokerServers2;

    @Value("${bootstrap.servers3}")
    private String BrokerServers3;

    @Value("${keytab.path}")
    private String keytabPath;
    @Value("${principal}")
    private String principal;

    @Bean
    KafkaTemplate<byte[], byte[]> kafkaTemplate1() {
        Map<String, Object> senderProps = consumerConfigs1();
        ProducerFactory<byte[], byte[]> pf =
                new DefaultKafkaProducerFactory<byte[], byte[]>(senderProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(pf);
        return template;
    }

    @Bean
    KafkaTemplate<byte[], byte[]> kafkaTemplate2() {
        Map<String, Object> senderProps = consumerConfigs2();
        ProducerFactory<byte[], byte[]> pf =
                new DefaultKafkaProducerFactory<byte[], byte[]>(senderProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(pf);
        return template;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory1")
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    kafkaListenerContainerFactory1() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory1());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        Properties properties = new Properties();
//        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        properties.setProperty("sasl.mechanism", "GSSAPI");
//        properties.setProperty("sasl.kerberos.service.name", "kafka");
        factory.getContainerProperties().setKafkaConsumerProperties(properties);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory1() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs1());
    }

    @Bean
    public Map<String, Object> consumerConfigs1() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServers1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "xx_client_kw");


//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "GSSAPI");
//        props.put("sasl.kerberos.service.name", "kafka");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServers3);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "xx_client_inner");

        return props;
    }


    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory2")
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    kafkaListenerContainerFactory2() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory2());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        //kerberos环境下的配置
//        Properties properties=new Properties();
//        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        properties.setProperty("sasl.mechanism", "GSSAPI");
//        properties.setProperty("sasl.kerberos.service.name", "kafka");
//        factory.getContainerProperties().setKafkaConsumerProperties(properties);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory2() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs2());
    }

    @Bean
    public Map<String, Object> consumerConfigs2() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServers3);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BrokerServers1);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

//        props.put(JaasConfig.getConfiguration())
        return props;
    }


    @Bean
    public KafkaJaasLoginModuleInitializer jaasConfig() throws IOException {
        KafkaJaasLoginModuleInitializer jaasConfig = new KafkaJaasLoginModuleInitializer();
        jaasConfig.setControlFlag(KafkaJaasLoginModuleInitializer.ControlFlag.REQUIRED);
        Map<String, String> options = new HashMap<>();
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("keyTab", keytabPath);
        options.put("principal", principal);
//        options.put("security.protocol", "SASL_PLAINTEXT");
//        options.put("sasl.mechanism", "GSSAPI");
//        options.put("sasl.kerberos.service.name", "kafka");
        jaasConfig.setOptions(options);
        return jaasConfig;
    }
}
