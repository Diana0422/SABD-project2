package com.diagiac.flink.query1.bean;

import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

@Data
public class KeyQuery1 {
    private Timestamp timestamp;
    private long sensorId;

    public static KeyQuery1 create(Timestamp ts, long sensorId){
        KeyQuery1 key = new KeyQuery1();
        key.setSensorId(sensorId);
        key.setTimestamp(ts);
        return key;
    }
}
