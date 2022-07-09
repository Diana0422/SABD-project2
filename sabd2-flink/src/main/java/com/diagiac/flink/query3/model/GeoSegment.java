package com.diagiac.flink.query3.model;

import lombok.Data;

import java.util.Objects;

/**
 * The GeoSegment represents a segment made by two GeoPoints.
 * Both Geopoints can be included or excluded by the segment.
 * It has also two fields that check if the segment is vertical or horizontal
 */
@Data
public class GeoSegment {
    private GeoPoint start;
    private GeoPoint end;
    private boolean horizontal;
    private boolean vertical;
    private boolean includedStart;
    private boolean includedEnd;

    /**
     * This constructor instantiate a GeoSegments and also check if it is vertical or horizontal
     * @param start starting GeoPoint
     * @param end ending GeoPoint
     * @param includedStart true if the start GeoPoint is included in the segment
     * @param includedEnd true if the end GeoPoint is included in the segment
     */
    public GeoSegment(GeoPoint start, GeoPoint end, boolean includedStart, boolean includedEnd) {
        this.start = start;
        this.end = end;
        this.includedStart = includedStart;
        this.includedEnd = includedEnd;
        this.horizontal = Objects.equals(start.getLat(), end.getLat());
        this.vertical = Objects.equals(start.getLon(), end.getLon());
    }


    /**
     * Check if the point is contained in the segment.
     * There are also checks on the two extreme points (they can be included or not).
     * Doesn't work for segments that are not vertical or horizontal.
     * @param p GeoPoint
     * @return true if it is inside
     */
    public boolean containsPoint(GeoPoint p) {
        // equal start and included
        boolean isStart = includedStart && p.equals(start);
        boolean isEnd = includedEnd && p.equals(end);

        boolean isInside = false;
        if (isVertical()) {
            isInside = p.getLon().equals(this.start.getLon()) && (p.getLat() > this.start.getLat() && p.getLat() < this.end.getLat());
        } else if (isHorizontal()) {
            isInside = p.getLat().equals(this.start.getLat()) && (p.getLon() > this.start.getLon() && p.getLon() < this.end.getLon());
        }
        return isStart || isEnd || isInside;
    }
}
