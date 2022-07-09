package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.Data;

import java.sql.Timestamp;

/**
 * Simple bean representing a sensor temperature measurement and the cell to which it belongs to.
 */
@Data
public class Query3Cell {
    private GeoCell cell;
    private int count;
    private Timestamp timestamp;
    private Double temperature;
    public Query3Cell(GeoCell cell, int count, Timestamp timestamp, Double temperature) {
        this.cell = cell;
        this.count = count;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }
}
