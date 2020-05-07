package pers.demo.springkafkaavro.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * avro 序列化  反序列化
 *
 * @Author Stsahana
 */
@Component
@Slf4j
public class AvroDeserializerUtil {

    /**
     * transfer kafka records to JsonArray
     * @param records
     * @param schema
     * @return
     */
    public JSONArray byte2Array(byte[] records, Schema schema) {
        List<Schema.Field> fields = schema.getFields();
        GenericData.Record propertyData = new GenericData.Record(schema);
        GenericDatumReader<GenericRecord> propertyReader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(records, null);
        GenericRecord propertyRecord;
        JSONArray jsonArray = new JSONArray();
        try {
            while (!decoder.isEnd()) {
                propertyRecord = propertyReader.read(propertyData, decoder);
                JSONObject jsonObject = avroToJSON(propertyRecord, schema.getFields());
                jsonArray.add(jsonObject);
            }

        } catch (Exception e) {
            log.error("Deserialize Exception:", e);
        }
        return jsonArray;
    }


    /**
     *  transfer record to json
     * @param genericRecord
     * @param fields
     * @return
     */
    public static JSONObject avroToJSON(GenericRecord genericRecord, List<Schema.Field> fields) {
        JSONObject jsonObject = new JSONObject();
        for (Schema.Field field:fields) {
            Schema schema = field.schema(); // Record中每个field的schema
            Object object = genericRecord.get(field.name()); // 每个位置的对象
            jsonObject.put(field.name(), toJSONStringForAvro(object, schema).toString()); // 第一层
        }
        return jsonObject;
    }

    /**
     *  decode object
     * @param object
     * @param schema
     * @return
     */
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
                jsonObject = avroToJSON(record, schema.getFields());
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
