package com.diagiac.flink.query3.util;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class GeoGridTest {

    @Test
    public void insideGrid() {
        GeoGrid geoGrid = GeoGrid.getInstance();

        // IN
        assertTrue(geoGrid.insideGrid(new GeoPoint(44.0, 7.0)));

        // OUT
        assertFalse(geoGrid.insideGrid(new GeoPoint(65.0, 15.0)), "UP point should not be inside"); // UP
        assertFalse(geoGrid.insideGrid(new GeoPoint(49.0, 40.0)), "RIGHT point should not be inside"); // RIGHT
        assertFalse(geoGrid.insideGrid(new GeoPoint(28.0, 15.0)), "DOWN point should not be inside"); // DOWN
        assertFalse(geoGrid.insideGrid(new GeoPoint(40.0, -8.0)), "LEFT point should not be inside"); // LEFT

        // BORDER
        assertTrue(geoGrid.insideGrid(new GeoPoint(45.0, 2.0)), "LEFT border point should be inside"); // LEFT
        assertTrue(geoGrid.insideGrid(new GeoPoint(38.0, 10.0)), "DOWN border point should be inside"); // DOWN
        assertTrue(geoGrid.insideGrid(new GeoPoint(46.0, 30.0)), "RIGHT border point should be inside"); // RIGHT
        assertTrue(geoGrid.insideGrid(new GeoPoint(58.0, 20.0)), "UP border point should be inside"); // UP

        // INSIDE with floating points
        assertFalse(geoGrid.insideGrid(new GeoPoint(65.60014, 12.9841)), "Point up outside should be inside"); // 13
        assertTrue(geoGrid.insideGrid(new GeoPoint(41.8681, 12.41944)), "Point in cell 1 should be inside"); // 1

        // CORNERS
        assertTrue(geoGrid.insideGrid(new GeoPoint(58.0, 2.0)), "Corner NW should be inside"); // NW
        assertTrue(geoGrid.insideGrid(new GeoPoint(58.0, 30.0)), "Corner NE should be inside"); // NE
        assertTrue(geoGrid.insideGrid(new GeoPoint(38.0, 2.0)), "Corner SW should be inside"); // SW
        assertTrue(geoGrid.insideGrid(new GeoPoint(38.0, 30.0)), "Corner SE should be inside"); // SE

    }

    @Test
    public void getContainingCellTest(){
        GeoGrid geoGrid = GeoGrid.getInstance();
        // internal to cell 0
        assertEquals(geoGrid.getCells().get(0), geoGrid.getContainingCell(new GeoPoint(40.57312, 5.5434)).orElse(null));
        // external to all cell
        assertEquals(Optional.empty(), geoGrid.getContainingCell(new GeoPoint(44.91019, -5.10112))); // 44.91019, -5.10112
        // borders 0
        assertEquals(geoGrid.getCells().get(0), geoGrid.getContainingCell(new GeoPoint(38.0, 6.0)).orElse(null)); // DOWN 0
        assertEquals(geoGrid.getCells().get(0), geoGrid.getContainingCell(new GeoPoint(40.0, 2.0)).orElse(null)); // LEFT 0
        assertEquals(geoGrid.getCells().get(4), geoGrid.getContainingCell(new GeoPoint(43.0, 6.0)).orElse(null)); // UP 0 -> 4
        assertEquals(geoGrid.getCells().get(1), geoGrid.getContainingCell(new GeoPoint(40.0, 9.0)).orElse(null)); // RIGHT 0 -> 1
        // border 3 SOUTH AND EAST
        assertEquals(geoGrid.getCells().get(3), geoGrid.getContainingCell(new GeoPoint(38.0, 28.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(3), geoGrid.getContainingCell(new GeoPoint(40.0, 30.0)).orElse(null));
        // border 7 SOUTH AND EAST
        assertEquals(geoGrid.getCells().get(7), geoGrid.getContainingCell(new GeoPoint(43.0, 28.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(7), geoGrid.getContainingCell(new GeoPoint(46.0, 30.0)).orElse(null));
        // border 11 SOURH AND EAST AND WEST
        assertEquals(geoGrid.getCells().get(11), geoGrid.getContainingCell(new GeoPoint(48.0, 28.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(11), geoGrid.getContainingCell(new GeoPoint(50.0, 30.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(11), geoGrid.getContainingCell(new GeoPoint(50.0, 23.0)).orElse(null));
        // border 15 SOUTH AND EAST AND WEST AND NORTH
        assertEquals(geoGrid.getCells().get(15), geoGrid.getContainingCell(new GeoPoint(53.0, 28.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(15), geoGrid.getContainingCell(new GeoPoint(56.0, 30.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(15), geoGrid.getContainingCell(new GeoPoint(56.0, 23.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(15), geoGrid.getContainingCell(new GeoPoint(58.0, 27.0)).orElse(null));
        // border 14 SOUTH AND WEST AND NORTH
        assertEquals(geoGrid.getCells().get(14), geoGrid.getContainingCell(new GeoPoint(53.0, 20.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(14), geoGrid.getContainingCell(new GeoPoint(56.0, 16.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(14), geoGrid.getContainingCell(new GeoPoint(58.0, 20.0)).orElse(null));

        // border 13 SOUTH AND WEST AND NORTH
        assertEquals(geoGrid.getCells().get(13), geoGrid.getContainingCell(new GeoPoint(53.0, 10.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(13), geoGrid.getContainingCell(new GeoPoint(56.0, 9.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(13), geoGrid.getContainingCell(new GeoPoint(58.0, 10.0)).orElse(null));

        // border 13 SOUTH AND WEST AND NORTH
        assertEquals(geoGrid.getCells().get(12), geoGrid.getContainingCell(new GeoPoint(53.0, 6.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(12), geoGrid.getContainingCell(new GeoPoint(56.0, 2.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(12), geoGrid.getContainingCell(new GeoPoint(58.0, 6.0)).orElse(null));

        // 4 outside corner
        assertEquals(geoGrid.getCells().get(0), geoGrid.getContainingCell(new GeoPoint(38.0, 2.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(3), geoGrid.getContainingCell(new GeoPoint(38.0, 30.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(12), geoGrid.getContainingCell(new GeoPoint(58.0, 2.0)).orElse(null));
        assertEquals(geoGrid.getCells().get(15), geoGrid.getContainingCell(new GeoPoint(58.0, 30.0)).orElse(null));
    }
}
