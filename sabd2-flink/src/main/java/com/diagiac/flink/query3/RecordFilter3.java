package com.diagiac.flink.query3;

import com.diagiac.flink.query3.bean.Query3Record;
import org.apache.flink.api.common.functions.FilterFunction;

import java.sql.Timestamp;

public class RecordFilter3 implements FilterFunction<Query3Record> {
    /**
     * Removes all values that have no temperature, no longitude, no latitude
     *
     * @param value The value to be filtered.
     * @return
     * @throws Exception
     */
    @Override
    public boolean filter(Query3Record value) throws Exception {
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
