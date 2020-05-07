package pers.demo.springkafkaavro.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;


@Component
@Slf4j
public class AvroSerializerUtil {

    /**
     * 将jsonObject封装成Schema定义的结构，复杂类型未做处理
     * https://avro.apache.org/docs/current/spec.html#schema_record
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
                propertyData.put(field.name(),toObject(field.schema(),jsonObject.get(field.name())));
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

    public Object toObject(Schema schema,Object object){
        Object result = null;
        switch (schema.getType()) {
            case LONG:
                result=TypeUtils.castToLong(object);
                break;
            case RECORD:
                GenericData.Record item = new GenericData.Record(schema);
                JSONObject jsonObject;
                if (object instanceof JSONObject) {
                    jsonObject= (JSONObject)object;
                } else if (object instanceof Map) {
                    jsonObject= new JSONObject((Map)object);
                } else {
                    jsonObject=object instanceof String ? JSON.parseObject((String)object) : (JSONObject)JSON.toJSON(object);
                }
                for (Schema.Field field:schema.getFields()){
                   item.put(field.name(),toObject(field.schema(),jsonObject.get(field.name())));
                }
                result= item;
                break;
            case ENUM:
                break;
            case ARRAY:
                List<Object> records = new ArrayList<>();
                JSONArray jsonArray;
                if (object instanceof JSONArray) {
                    jsonArray=(JSONArray)object;
                } else if (object instanceof List) {
                    jsonArray=new JSONArray((List)object);
                } else {
                    jsonArray= object instanceof String ? (JSONArray)JSON.parse((String)object) : (JSONArray)JSON.toJSON(object);
                }
                Schema itemSchema = schema.getElementType();
                for(int i=0;i<jsonArray.size();i++){
                    Object o=toObject(itemSchema,jsonArray.get(i));
//                    item.put();
                    records.add(o);
                }
                result=records;
                break;
            case MAP:
                result=JSON.parseObject(object.toString()).getInnerMap();
                break;
            case UNION://按string处理-----此处有问题
//              result=(String) object;
                break;
            case FIXED://定长字节
                result=TypeUtils.castToByte(object);
                break;
            case STRING:
                result=(String) object;
                break;
            case BYTES:
                result=TypeUtils.castToByte(object);
                break;
            case INT:
                result=TypeUtils.castToInt(object);
                break;
            case BOOLEAN:
                result=TypeUtils.castToBoolean(object);
                break;
            case FLOAT:
                result=TypeUtils.castToFloat(object);
                break;
            case DOUBLE:
                result=TypeUtils.castToDouble(object);
                break;
            case NULL:
                result=null;
                break;
        }
        return  result;
    }

    /**
     * jsonArray 转换成byte
     * @param schema
     * @param jsonArray
     * @return
     */
    public byte[] array2Byte(Schema schema, JSONArray jsonArray) {
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutS = new ByteArrayOutputStream(1024 * 1024 * 4);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutS, null);
        try {
            for (int i = 0; i < jsonArray.size(); i++) {
                Object object = jsonArray.get(i);
                Object result = toObject(schema, object);
                // write to encoder
                writer.write(result, binaryEncoder);
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
