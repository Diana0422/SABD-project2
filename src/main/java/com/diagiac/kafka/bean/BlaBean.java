package com.diagiac.kafka.bean;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;

@Data
public class BlaBean {
    @CsvBindByName
    private String sensor_id;
    @CsvBindByName
    private String sensor_type;
    @CsvBindByName
    private String location;
    @CsvBindByName
    private String lat;
    @CsvBindByName
    private String lon;
    @CsvBindByName
    private String timestamp;
    @CsvBindByName
    private String pressure;
    @CsvBindByName
    private String altitude;
    @CsvBindByName
    private String pressure_sealevel;
    @CsvBindByName
    private String temperature;
}
