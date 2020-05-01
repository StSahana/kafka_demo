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
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * avro 序列化  反序列化
 *
 * @Author Stsahana
 */
@Component
@Slf4j
public class AvroUtil {


    /**
     * 将jsonObject封装成Schema
     * 只有一层的schema结构，复杂类型未做处理
     * @param schema
     * @param jsonObject
     * @return
     */
    public byte[] object2Byte(Schema schema, JSONObject jsonObject) {
        GenericData.Record propertyData = new GenericData.Record(schema);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        List<Schema.Field> fields = schema.getFields();
        ByteArrayOutputStream byteArrayOutS = new ByteArrayOutputStream(1024 * 1024 * 4);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutS, null);
        try {
            for (Schema.Field field : fields) {
                log.debug("field:{}", field.name(), field.schema());
                switch (field.schema().getType().toString()) {
                    case "LONG":
                        propertyData.put(field.name(), jsonObject.getLongValue(field.name()));
                        break;
                    case "INT":
                        propertyData.put(field.name(), jsonObject.getIntValue(field.name()));
                        break;
                    default:
                        propertyData.put(field.name(), jsonObject.getString(field.name()));
                        break;
                }
            }
            // write to encoder
            writer.write(propertyData, binaryEncoder);
            binaryEncoder.flush();
            byteArrayOutS.flush();
            return byteArrayOutS.toByteArray();
        } catch (Exception e) {
            log.error("Encoder exception:", e);
        }
        return null;
    }


    /**
     * 只有一层的schema结构，复杂类型未做处理
     *
     * @param schema
     * @param jsonArray
     * @return
     */
    public byte[] array2Byte(Schema schema, JSONArray jsonArray) {
        GenericData.Record propertyData = new GenericData.Record(schema);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        List<Schema.Field> fields = schema.getFields();
        ByteArrayOutputStream byteArrayOutS = new ByteArrayOutputStream(1024 * 1024 * 4);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutS, null);
        try {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                for (Schema.Field field : fields) {
                    log.debug("field:{}", field.name(), field.schema());
                    if (jsonObject.containsKey(field.name()) && null == jsonObject.getString(field.name())) {
                        propertyData.put(field.name(), "NULL");
                        continue;
                    }
                    switch (field.schema().getType().toString()) {
                        case "LONG":
                            propertyData.put(field.name(), jsonObject.getLongValue(field.name()));
                            break;
                        case "INT":
                            propertyData.put(field.name(), jsonObject.getIntValue(field.name()));
                            break;
                        default:
                            propertyData.put(field.name(), jsonObject.getString(field.name()));
                            break;
                    }
                }
                // write to encoder
                writer.write(propertyData, binaryEncoder);
            }
            binaryEncoder.flush();
            byteArrayOutS.flush();
            return byteArrayOutS.toByteArray();
        } catch (Exception e) {
            log.error("Encoder exception:", e);
        }
        return null;
    }

    /**
     * 仅一层结构的反序列化，复杂类型未做处理
     *
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
                JSONObject jsonObject = AvroJsonUtil.avroToJSON(propertyRecord, schema.getFields());
//                JSONObject jsonObject = new JSONObject();
//                if (null != propertyRecord) {
//                    for (Schema.Field field : fields) {
//                        if (null != propertyRecord.get(field.name())) {
//                            jsonObject.put(field.name(), propertyRecord.get(field.name()).toString());
//                        } else {
//                            jsonObject.put(field.name(), "NULL");
//                        }
//                    }
//                }
                jsonArray.add(jsonObject);
            }

        } catch (Exception e) {
            log.error("Deserialize Exception:", e);
        }
        return jsonArray;
    }

    /**
     *  根据schema获取随机内容
     * @param schema
     * @return
     */
    public static Object getDefaultData(Schema schema) {
        Object data = null;
        switch (schema.getType()) {
            case INT:
                data = 1;
                break;
            case LONG:
                data = 1234567890123L;
                break;
            case BOOLEAN:
                data = true;
                break;
            case FLOAT:
                data = 1.0f;
                break;
            case DOUBLE:
                data = 1.0d;
                break;
            case STRING:
                data = "test-string";
                break;
            case FIXED:
                data = ByteBuffer.wrap("test-fixed".getBytes());
                break;
            case BYTES:
                data = ByteBuffer.wrap("test-bytes".getBytes());
                break;
            case ENUM:
                data = schema.getEnumSymbols().get(0);
                break;
            case ARRAY:
                Object element = getDefaultData(schema.getElementType());
                data = Arrays.asList(element);
                break;
            case MAP:
                Object value = getDefaultData(schema.getValueType());
                data = new HashMap<String, Object>();
                ((HashMap) data).put("mapKey", value);
                break;
            case UNION:
                for (Schema unionSchema : schema.getTypes()) {
                    if (unionSchema.getType() != Schema.Type.NULL) {
                        data = getDefaultData(unionSchema);
                        break;
                    }
                }
            case RECORD:
                List<Schema.Field> fields = schema.getFields();
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : fields) {
                    record.put(field.name(),getDefaultData(field.schema()));
                }
                data = record;
                break;
        }
        return data;
    }
}
