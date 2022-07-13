package com.diagiac.flink.query3.util;

import com.diagiac.flink.query3.model.GeoGrid;
import com.diagiac.flink.query3.model.GeoPoint;
import com.diagiac.flink.query3.model.GeoSegment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeoSegmentTest {

    /**
     * Checks if geoSegment.containsPoint() method works.
     */
    @Test
    public void containsPoint(){
        GeoGrid g = GeoGrid.getInstance();

        // ==== horizontal start end included ====
        GeoSegment horizSegment = g.getCells().get(3).getIncludedSegments().get(1);
        // internal
        assertTrue(horizSegment.containsPoint(new GeoPoint(53.0, 26.0)));
        // border right and left
        assertTrue(horizSegment.containsPoint(new GeoPoint(53.0, 30.0)));
        assertTrue(horizSegment.containsPoint(new GeoPoint(53.0, 23.0)));
        assertFalse(horizSegment.containsPoint(new GeoPoint(53.0, 16.0)));
        assertFalse(horizSegment.containsPoint(new GeoPoint(53.0, 31.0)));

        // vertical cell 1
        GeoSegment seg1 = g.getCells().get(13).getIncludedSegments().get(1);
        // internal
        assertTrue(seg1.containsPoint(new GeoPoint(40.0, 9.0)));
        // border included
        assertTrue(seg1.containsPoint(new GeoPoint(38.0, 9.0)));
        // border excluded
        assertFalse(seg1.containsPoint(new GeoPoint(43.0, 9.0)));
        // external
        assertFalse(seg1.containsPoint(new GeoPoint(37.0, 9.0)));
        assertFalse(seg1.containsPoint(new GeoPoint(48.0, 9.0)));
    }
}
