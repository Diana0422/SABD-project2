package com.diagiac.flink.query3.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.diagiac.flink.query3.model.GeoCell.CellType.*;

@Data
public class GeoGrid {
    private static GeoGrid instance = null;
    private final List<GeoCell> cells;

    public synchronized static GeoGrid getInstance() {
        if (instance == null) {
            instance = new GeoGrid();
        }
        return instance;
    }


    /**
     * 12 13 14 15
     * 8  9  10 11
     * 4  5  6  7
     * 0  1  2  3
     */
    public GeoGrid() {
        cells = new ArrayList<>(16);
        // South West + North East (lat, lon)
        cells.add(new GeoCell(0, new GeoPoint(38.0, 2.0), new GeoPoint(43.0, 9.0), TwoBorders)); // 0
        cells.add(new GeoCell(1, new GeoPoint(38.0, 9.0), new GeoPoint(43.0, 16.0), TwoBorders)); // 1
        cells.add(new GeoCell(2, new GeoPoint(38.0, 16.0), new GeoPoint(43.0, 23.0), TwoBorders)); // 2
        cells.add(new GeoCell(3, new GeoPoint(38.0, 23.0), new GeoPoint(43.0, 30.0), ThreeBordersEast)); // 3

        cells.add(new GeoCell(4, new GeoPoint(43.0, 2.0), new GeoPoint(48.0, 9.0), TwoBorders)); // 4
        cells.add(new GeoCell(5, new GeoPoint(43.0, 9.0), new GeoPoint(48.0, 16.0), TwoBorders)); // 5
        cells.add(new GeoCell(6, new GeoPoint(43.0, 16.0), new GeoPoint(48.0, 23.0), TwoBorders)); // 6
        cells.add(new GeoCell(7, new GeoPoint(43.0, 23.0), new GeoPoint(48.0, 30.0), ThreeBordersEast)); // 7

        cells.add(new GeoCell(8, new GeoPoint(48.0, 2.0), new GeoPoint(53.0, 9.0), TwoBorders)); // 8
        cells.add(new GeoCell(9, new GeoPoint(48.0, 9.0), new GeoPoint(53.0, 16.0), TwoBorders)); // 9
        cells.add(new GeoCell(10, new GeoPoint(48.0, 16.0), new GeoPoint(53.0, 23.0), TwoBorders)); // 10
        cells.add(new GeoCell(11, new GeoPoint(48.0, 23.0), new GeoPoint(53.0, 30.0), ThreeBordersEast)); // 11

        cells.add(new GeoCell(12, new GeoPoint(53.0, 2.0), new GeoPoint(58.0, 9.0), ThreeBordersNorth)); // 12
        cells.add(new GeoCell(13, new GeoPoint(53.0, 9.0), new GeoPoint(58.0, 16.0), ThreeBordersNorth)); // 13
        cells.add(new GeoCell(14, new GeoPoint(53.0, 16.0), new GeoPoint(58.0, 23.0), ThreeBordersNorth)); // 14
        cells.add(new GeoCell(15, new GeoPoint(53.0, 23.0), new GeoPoint(58.0, 30.0), FourBorders)); // 15
    }

    public boolean insideGrid(GeoPoint p) {
        // latitude between 38 and 58
        // longitude between 2 and 30
        return p.getLat() >= 38.0 && p.getLat() <= 58.0 && p.getLon() >= 2.0 && p.getLon() <= 30.0;
    }


    /**
     * Gets the cell that contiains the point.
     * Every cell contains points that are on Left and Down Border.
     *
     * @param p
     * @return
     */
    public Optional<GeoCell> getContainingCell(GeoPoint p) {
        if (!insideGrid(p)) {
            return Optional.empty();
        }

        for (GeoCell cell : cells) {
            if (cell.containsGeoPoint(p)) {
                return Optional.of(cell);
            }
        }
        return Optional.empty();
    }
}
