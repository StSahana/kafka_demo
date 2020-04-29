package pers.demo.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;

public class HttpCsv {
    public static void main(String[] args) throws Exception {

        // HTTP
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // get http server list
        int curHTTPServer = 0;
        // HTTP
        String[] ips = new String[]{"http://192.168.2.2:10080", "http://192.168.2.1:10080"};

        // get schema
        String topic = "user";

        /**
         * method 1:
         * 该处为使用schema-registry客户端获取schema
         */
        Schema.Parser parser = new Schema.Parser();
        CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient("http://192.168.2.3:8081", 100);
        Schema schema = parser.parse(cachedSchemaRegistryClient.getLatestSchemaMetadata(topic).getSchema());


        // ~10MB
        ByteArrayOutputStream out = new ByteArrayOutputStream(10000000);

        for (int i = 0; i < 1; i++) {
            // 构造一批数据,推荐大小为5MB左右
            out.reset();
            for (int j = 0; j < 20000; j++) {
                //写数据
                String s = "a\t\t1.1.1.1\t1.1.1.1\t3.2.1.1,61.147.37.1\t4\t4\tAAAA\t0\t2019-09-03 00:01:50.285\t1546355535\n";
                out.write(s.getBytes());
            }

            out.flush();
            HttpResponse response = null;
            try {
                // load balance
                HttpPost request = new HttpPost(ips[curHTTPServer]);
                curHTTPServer++;
                curHTTPServer %= ips.length;
                System.out.println("server: " + ips[curHTTPServer]);

                // set header
                request.addHeader("Content-type", "utf8");
                request.addHeader("User", "LiMing");
                request.addHeader("Password", "123");
                request.addHeader("Field-Split", "\\t");
                request.addHeader("Row-Split", "\\n");
                request.addHeader("Topic", topic);
                request.addHeader("Format", "csv");
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
