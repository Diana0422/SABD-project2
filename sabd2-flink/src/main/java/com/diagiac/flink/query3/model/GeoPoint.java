package com.diagiac.flink.query3.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Objects;

/**
 * Simple entity/model class that represents a geographic point with latitude and longitude
 */
@Data
@AllArgsConstructor
public class GeoPoint {
    private Double lat; // horizontal
    private Double lon; // vertical

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoPoint geoPoint = (GeoPoint) o;
        return Objects.equals(lat, geoPoint.lat) && Objects.equals(lon, geoPoint.lon);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lat, lon);
    }
}
