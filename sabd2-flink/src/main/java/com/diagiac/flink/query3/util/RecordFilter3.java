package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.Query3Record;
import org.apache.flink.api.common.functions.FilterFunction;

import java.sql.Timestamp;

/**
 * Removes all values that have no temperature, no longitude, no latitude
 * and also the outlier temperatures (too low or too high)
 */
public class RecordFilter3 implements FilterFunction<Query3Record> {
    @Override
    public boolean filter(Query3Record value) {
        Double temperature = value.getTemperature();
        Double latitude = value.getLatitude();
        Double longitude = value.getLongitude();
        Timestamp timestamp = value.getTimestamp();
        boolean temperatureIsPresent = temperature != null;
        boolean latitudeIsPresent = latitude != null;
        boolean longitudeIsPresent = longitude != null;
        boolean timestampIsPresent = timestamp != null;

        if (temperatureIsPresent) {
            return temperature > -93.2 && temperature < 56.7
                   && latitudeIsPresent
                   && longitudeIsPresent
                   && timestampIsPresent;
        } else {
            return false;
        }
    }
}
