package pers.demo.kafka;

import org.apache.avro.Schema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class SchemaTest {

    public static void main(String[] args) throws IOException, RestClientException {
       Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://192.168.2.3:8081", 100);
        org.apache.avro.Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata("http_data").getSchema());
        for (Schema.Field field : schema.getFields()) {
                    System.out.println(field.name());
                    System.out.println(field.schema().getType().getName());
                    if(field.schema().getType().getName().equals("union")){
                        System.out.println(field.schema().getTypes().get(0).getName());
                    }
                }

    }
}
