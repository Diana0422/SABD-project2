package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.bean.Query3Cell;
import com.diagiac.flink.query3.bean.Query3Record;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Optional;

public class CellMapper implements MapFunction<Query3Record, Query3Cell> {
    @Override
    public Query3Cell map(Query3Record record) throws Exception {
        Double latitude = record.getLatitude();
        Double longitude = record.getLongitude();

        GeoPoint geoPoint = new GeoPoint(latitude, longitude);
        GeoGrid geoGrid = GeoGrid.getInstance();
        Optional<GeoCell> containingCell = geoGrid.getContainingCell(geoPoint);
        return new Query3Cell(containingCell.orElse(null), 1, record.getTimestamp(), record.getTemperature());
    }
}
