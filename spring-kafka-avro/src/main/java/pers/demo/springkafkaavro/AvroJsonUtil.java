package pers.demo.springkafkaavro;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Avro to json object util class. */
public class AvroJsonUtil {
    /** Transform the generic record to json object by schema info. */
    public static JSONObject avroToJSON(GenericRecord genericRecord, List<Schema.Field> fields) {
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < fields.size(); i++) {
            Schema schema = fields.get(i).schema(); // Record中每个field的schema
            Object object = genericRecord.get(i); // 每个位置的对象
            jsonObject.put(fields.get(i).name(), toJSONStringForAvro(object, schema).toString()); // 第一层
        }
        return jsonObject;
    }

    private static Object toJSONStringForAvro(Object object, Schema schema) {
        if (null == object) {
            return "NULL";
        }
        JSONObject jsonObject;

        switch (schema.getType()) {
            case ARRAY:
                JSONArray jsonArray = new JSONArray();
                GenericData.Array array = (GenericData.Array) object;
                for (Object o : array) {
                    jsonArray.add(toJSONStringForAvro(o, schema.getElementType()));
                }
                return jsonArray;
            case MAP:
                Map<Utf8, Object> map = (Map<Utf8, Object>) object;
                jsonObject = new JSONObject();
                for (Map.Entry<Utf8, Object> entry : map.entrySet()) {
                    jsonObject.put(
                            entry.getKey().toString(),
                            toJSONStringForAvro(entry.getValue(), schema.getValueType()));
                }
                return jsonObject;
            case RECORD:
                GenericRecord record = (GenericRecord) object;
                jsonObject = new JSONObject();
                List<Schema.Field> fields = record.getSchema().getFields();
                for (int i = 0; i < fields.size(); i++) {
                    jsonObject.put(
                            fields.get(i).name(), toJSONStringForAvro(record.get(i), fields.get(i).schema()));
                }
                return jsonObject;
            case BYTES:
                ByteBuffer byteBuffer = (ByteBuffer) object;
                byte[] binary = new byte[byteBuffer.capacity()];
                byteBuffer.get(binary, 0, binary.length);
                String string = new String(binary, StandardCharsets.UTF_8);
                byteBuffer.clear();
                return string;
            default:
                return String.valueOf(object);
        }
    }
}
