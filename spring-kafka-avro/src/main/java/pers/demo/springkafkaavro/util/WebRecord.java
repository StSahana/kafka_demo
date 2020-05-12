package pers.demo.springkafkaavro.util;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WebRecord {
    private String phone;
    private String imei;
    private String imsi;
    private String mac ;
    private String url;
    private int time;
}
