package pers.demo.springkafkaavro.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class SchemaUtil {
    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;
    private Map<String,Schema> schemaMap=new ConcurrentHashMap<String,Schema>();

   public Schema getSchema(String topic){
       if (schemaMap.containsKey(topic)){
           return schemaMap.get(topic);
       }else {
           Schema.Parser parser = new Schema.Parser();
           CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
           Schema schema = null;
           try {
               String schemaString = cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema();
               schema = parser.parse(schemaString);
               log.debug("topic:{},schema:{}",topic,schema);
           } catch (IOException e) {
               log.error("get {} schema failed<IOException>:{}", topic, e.getLocalizedMessage());
               e.printStackTrace();
           } catch (RestClientException e) {
               log.error("get {} schema failed<RestClientException>:{}", topic, e.getLocalizedMessage());
               e.printStackTrace();
           }
           schemaMap.put(topic,schema);
           return schema;
       }
   }

}
