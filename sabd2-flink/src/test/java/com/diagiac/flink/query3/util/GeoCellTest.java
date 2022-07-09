package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.model.GeoCell;
import com.diagiac.flink.query3.model.GeoGrid;
import com.diagiac.flink.query3.model.GeoPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeoCellTest {

    /**
     * Tests the geoCell.containsGeoPoint method
     */
    @Test
    public void containsGeoPointTest(){
        GeoGrid geoGrid = GeoGrid.getInstance();
        GeoCell geoCell0 = geoGrid.getCells().get(0);

        // IN
        assertTrue(geoCell0.containsGeoPoint(new GeoPoint(41.0, 7.0)));

        // OUT
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(44.92577, 5.50357)), "UP point should not be inside"); // UP
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(40.81053, 12.57732)), "RIGHT point should not be inside"); // RIGHT
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(35.40502, 5.48551)), "DOWN point should not be inside"); // DOWN
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(40.33421, -3.68017)), "LEFT point should not be inside"); // LEFT

        // BORDER 0
        assertTrue(geoCell0.containsGeoPoint(new GeoPoint(40.0, 2.0)), "LEFT border point should be inside"); // LEFT
        assertTrue(geoCell0.containsGeoPoint(new GeoPoint(38.0, 6.0)), "DOWN border point should be inside"); // DOWN
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(40.0, 9.0)), "RIGHT border point should be inside"); // RIGHT
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(43.0, 6.0)), "UP border point should be inside"); // UP

        // OUTSIDE with floating points
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(42.87958, -5.1411)), "Point up outside should be inside"); // 13
        // INSIDE with floating points
        assertTrue(geoCell0.containsGeoPoint(new GeoPoint(41.38241, 6.16038)), "Point in cell 1 should be inside"); // 1

        // CORNERS 0
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(43.0, 2.0)), "Corner NW should be inside"); // NW
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(43.0, 9.0)), "Corner NE should be inside"); // NE
        assertTrue(geoCell0.containsGeoPoint(new GeoPoint(38.0, 2.0)), "Corner SW should be inside"); // SW
        assertFalse(geoCell0.containsGeoPoint(new GeoPoint(38.0, 9.0)), "Corner SE should be inside"); // SE

        GeoCell geoCell3 = geoGrid.getCells().get(3);
        // CORNERS 3
        assertTrue(geoCell3.containsGeoPoint(new GeoPoint(38.0, 23.0)), "Corner SW should be inside");
        assertTrue(geoCell3.containsGeoPoint(new GeoPoint(38.0, 30.0)), "Corner SE should be inside");
        assertFalse(geoCell3.containsGeoPoint(new GeoPoint(43.0, 23.0)), "Corner NW should be inside");
        assertFalse(geoCell3.containsGeoPoint(new GeoPoint(43.0, 30.0)), "Corner NE should be inside");

        // BORDER 3
        assertTrue(geoCell3.containsGeoPoint(new GeoPoint(38.0, 26.0)), "SUD should be inside");
        assertTrue(geoCell3.containsGeoPoint(new GeoPoint(40.0, 23.0)), "LEFT should be inside");
        assertTrue(geoCell3.containsGeoPoint(new GeoPoint(40.0, 30.0)), "RIGHT should be inside");
        assertFalse(geoCell3.containsGeoPoint(new GeoPoint(43.0, 26.0)), "UP  should NOT be inside");

    }
}
