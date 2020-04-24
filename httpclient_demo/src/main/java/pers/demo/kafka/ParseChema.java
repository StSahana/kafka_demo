package pers.demo.kafka;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class ParseChema {

    public static void main(String[] args) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("D:\\Users\\StSahana\\Desktop\\http_data.schema"));
        for (Schema.Field field : schema.getFields()) {
            System.out.println(field.name());
            System.out.println(field.schema().getType().getName());
            if(field.schema().getType().getName().equals("union")){
                System.out.println(field.schema().getTypes().get(0).getName());
            }
        }
    }
}
