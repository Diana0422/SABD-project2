package com.diagiac.flink.query3.bean;

import com.diagiac.flink.query3.model.GeoCell;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

/**
 * ts, cell_0, avg_temp0, med_temp0, ... cell_15, avg_temp15, med_temp15
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Query3Result {
    private Timestamp timestamp;
    private GeoCell cell0;
    private Double avgTemp0;
    private Double medianTemp0;
    private GeoCell cell1;
    private Double avgTemp1;
    private Double medianTemp1;
    private GeoCell cell2;
    private Double avgTemp2;
    private Double medianTemp2;
    private GeoCell cell3;
    private Double avgTemp3;
    private Double medianTemp3;
    private GeoCell cell4;
    private Double avgTemp4;
    private Double medianTemp4;
    private GeoCell cell5;
    private Double avgTemp5;
    private Double medianTemp5;
    private GeoCell cell6;
    private Double avgTemp6;
    private Double medianTemp6;
    private GeoCell cell7;
    private Double avgTemp7;
    private Double medianTemp7;
    private GeoCell cell8;
    private Double avgTemp8;
    private Double medianTemp8;
    private GeoCell cell9;
    private Double avgTemp9;
    private Double medianTemp9;
    private GeoCell cell10;
    private Double avgTemp10;
    private Double medianTemp10;
    private GeoCell cell11;
    private Double avgTemp11;
    private Double medianTemp11;
    private GeoCell cell12;
    private Double avgTemp12;
    private Double medianTemp12;
    private GeoCell cell13;
    private Double avgTemp13;
    private Double medianTemp13;
    private GeoCell cell14;
    private Double avgTemp14;
    private Double medianTemp14;
    private GeoCell cell15;
    private Double avgTemp15;
    private Double medianTemp15;

    public Query3Result(CellAvgMedianTemperature[] elements) {
        timestamp = Arrays.stream(elements)
                .map(CellAvgMedianTemperature::getTimestamp)
                .filter(Objects::nonNull).findFirst()
                .orElse(null);
        cell0 = elements[0].getCell();
        cell1 = elements[1].getCell();
        cell2 = elements[2].getCell();
        cell3 = elements[3].getCell();
        cell4 = elements[4].getCell();
        cell5 = elements[5].getCell();
        cell6 = elements[6].getCell();
        cell7 = elements[7].getCell();
        cell8 = elements[8].getCell();
        cell9 = elements[9].getCell();
        cell10 = elements[10].getCell();
        cell11 = elements[11].getCell();
        cell12 = elements[12].getCell();
        cell13 = elements[13].getCell();
        cell14 = elements[14].getCell();
        cell15 = elements[15].getCell();
        avgTemp0 = elements[0].getAvgTemperature();
        avgTemp1 = elements[1].getAvgTemperature();
        avgTemp2 = elements[2].getAvgTemperature();
        avgTemp3 = elements[3].getAvgTemperature();
        avgTemp4 = elements[4].getAvgTemperature();
        avgTemp5 = elements[5].getAvgTemperature();
        avgTemp6 = elements[6].getAvgTemperature();
        avgTemp7 = elements[7].getAvgTemperature();
        avgTemp8 = elements[8].getAvgTemperature();
        avgTemp9 = elements[9].getAvgTemperature();
        avgTemp10 = elements[10].getAvgTemperature();
        avgTemp11 = elements[11].getAvgTemperature();
        avgTemp12 = elements[12].getAvgTemperature();
        avgTemp13 = elements[13].getAvgTemperature();
        avgTemp14 = elements[14].getAvgTemperature();
        avgTemp15 = elements[15].getAvgTemperature();
        medianTemp0 = elements[0].getMedianTemperature();
        medianTemp1 = elements[1].getMedianTemperature();
        medianTemp2 = elements[2].getMedianTemperature();
        medianTemp3 = elements[3].getMedianTemperature();
        medianTemp4 = elements[4].getMedianTemperature();
        medianTemp5 = elements[5].getMedianTemperature();
        medianTemp6 = elements[6].getMedianTemperature();
        medianTemp7 = elements[7].getMedianTemperature();
        medianTemp8 = elements[8].getMedianTemperature();
        medianTemp9 = elements[9].getMedianTemperature();
        medianTemp10 = elements[10].getMedianTemperature();
        medianTemp11 = elements[11].getMedianTemperature();
        medianTemp12 = elements[12].getMedianTemperature();
        medianTemp13 = elements[13].getMedianTemperature();
        medianTemp14 = elements[14].getMedianTemperature();
        medianTemp15 = elements[15].getMedianTemperature();
    }

    @Override
    public String toString() {
        return "Query3Result{" +
               "timestamp=" + timestamp +
               ", cell0=" + cell0 +
               ", avgTemp0=" + avgTemp0 +
               ", medianTemp0=" + medianTemp0 +
               ", cell1=" + cell1 +
               ", avgTemp1=" + avgTemp1 +
               ", medianTemp1=" + medianTemp1 +
               ", cell2=" + cell2 +
               ", avgTemp2=" + avgTemp2 +
               ", medianTemp2=" + medianTemp2 +
               ", cell3=" + cell3 +
               ", avgTemp3=" + avgTemp3 +
               ", medianTemp3=" + medianTemp3 +
               ", cell4=" + cell4 +
               ", avgTemp4=" + avgTemp4 +
               ", medianTemp4=" + medianTemp4 +
               ", cell5=" + cell5 +
               ", avgTemp5=" + avgTemp5 +
               ", medianTemp5=" + medianTemp5 +
               ", cell6=" + cell6 +
               ", avgTemp6=" + avgTemp6 +
               ", medianTemp6=" + medianTemp6 +
               ", cell7=" + cell7 +
               ", avgTemp7=" + avgTemp7 +
               ", medianTemp7=" + medianTemp7 +
               ", cell8=" + cell8 +
               ", avgTemp8=" + avgTemp8 +
               ", medianTemp8=" + medianTemp8 +
               ", cell9=" + cell9 +
               ", avgTemp9=" + avgTemp9 +
               ", medianTemp9=" + medianTemp9 +
               ", cell10=" + cell10 +
               ", avgTemp10=" + avgTemp10 +
               ", medianTemp10=" + medianTemp10 +
               ", cell11=" + cell11 +
               ", avgTemp11=" + avgTemp11 +
               ", medianTemp11=" + medianTemp11 +
               ", cell12=" + cell12 +
               ", avgTemp12=" + avgTemp12 +
               ", medianTemp12=" + medianTemp12 +
               ", cell13=" + cell13 +
               ", avgTemp13=" + avgTemp13 +
               ", medianTemp13=" + medianTemp13 +
               ", cell14=" + cell14 +
               ", avgTemp14=" + avgTemp14 +
               ", medianTemp14=" + medianTemp14 +
               ", cell15=" + cell15 +
               ", avgTemp15=" + avgTemp15 +
               ", medianTemp15=" + medianTemp15 +
               '}';
    }
}
