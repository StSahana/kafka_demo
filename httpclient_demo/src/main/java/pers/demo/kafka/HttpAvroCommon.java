package pers.demo.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;

public class HttpAvroCommon {
    public static void main(String[] args) throws Exception {

        // HTTP
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // get http server list
        int curHTTPServer = 0;
        // HTTP
        String[] ips = new String[]{"http://192.168.2.205:10080"};

        // get schema
        String topic = "http_data";

        /**
         * method 1:
         * 该处为使用schema-registry客户端获取schema
         */
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://192.168.2.3:8081", 100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());

        //数据序列化
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        // ~10MB
        ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        for (int i = 0; i < 1; i++) {
            // 构造一批数据,推荐大小为5MB左右
            out.reset();
            for (int j = 0; j < 10; j++) {
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    if (field.schema().getType().getName().equals("string")) {
                        record.put(field.name(), "stringTest");
                    } else if (field.schema().getType().getName().equals("union")) {
                        if (field.schema().getTypes().get(0).getName().equals("string")) {
                            record.put(field.name(), "stringTestUnion");
                        }
                        if (field.schema().getTypes().get(0).equals("int")) {
                            record.put(field.name(), 123);
                        }
                    }
                }
                writer.write(record, encoder);
//                record.put("host","xx.baidu.com");
//                record.put("cname","");
//                record.put("destip","a.a.a.a");
//                record.put("srcip","1.1.1.1");
//                record.put("dnsip","61.177.7.1");
//                record.put("iptype",4);
//                record.put("count",4);
//                record.put("timestamp",1546355535);
//                record.put("accesstime","2019-09-03 00:01:50.285");
//                record.put("reqtype","AAAA");
//                record.put("recursion",0);
//                writer.write(record, encoder);
            }
            // send to http server
            encoder.flush();
            out.flush();
            HttpResponse response = null;
            try {
                // load balance
                HttpPost request = new HttpPost(ips[curHTTPServer]);
                curHTTPServer++;
                curHTTPServer %= ips.length;
                System.out.println("server: " + ips[curHTTPServer]);

                // set header
                request.addHeader("content-type", " binary/octet-stream");
                request.addHeader("User", "LiMing");
                request.addHeader("Password", "123");
                request.addHeader("Topic", topic);
                request.addHeader("Format", "avro");
                HttpEntity httpEntity = new ByteArrayEntity(out.toByteArray());
                request.setEntity(httpEntity);
                System.out.println("request:" + request);
                //do not skip it!!!!!
                response = httpclient.execute(request);
                System.out.println("reponse:" + response);
                System.out.println(response.getStatusLine().getStatusCode());
            } catch (Exception ex) {
                // handle response here... try other servers
                ex.printStackTrace();
            } finally {
                response.getEntity().getContent().close();
            }
        }
        if (httpclient != null) {
            httpclient.close();
        }
    }
}
