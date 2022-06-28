package com.diagiac.flink.query3.util;

import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Data
public class GeoCell {
    private int id;
    private GeoPoint NW; // NW ----- NE
    private GeoPoint NE; // |        |
    private GeoPoint SW; // |        |
    private GeoPoint SE; // SW ----- SE

    private List<GeoSegment> includedSegments;

    public GeoCell(GeoPoint southWest, GeoPoint northEast, CellType cellType) {
        this.NW = new GeoPoint(northEast.getLat(), southWest.getLon());
        this.NE = northEast;
        this.SW = southWest;
        this.SE = new GeoPoint(southWest.getLat(), northEast.getLon());
        switch (cellType) {
            case TwoBorders:
                this.includedSegments = Arrays.asList(
                        new GeoSegment(SW, SE, true, false),
                        new GeoSegment(SW, NW, true, false)
                );
                break;
            case ThreeBordersEast:
                this.includedSegments = Arrays.asList(
                        new GeoSegment(SW, NW, true, false),
                        new GeoSegment(SW, SE, true, true),
                        new GeoSegment(SE, NE, true, false)
                );
                break;
            case ThreeBordersNorth:
                this.includedSegments = Arrays.asList(
                        new GeoSegment(SW, NW, true, true),
                        new GeoSegment(SW, SE, true, false),
                        new GeoSegment(NW, NE, true, false)
                );
                break;
            case FourBorders:
                this.includedSegments = Arrays.asList(
                        new GeoSegment(SW, NW, true, true), // vert
                        new GeoSegment(SW, SE, true, true), // hor
                        new GeoSegment(NW, NE, true, true), // hor
                        new GeoSegment(SE, NE, true, true) // vert
                );
                break;
        }
    }

    public boolean containsGeoPoint(GeoPoint p) {
        boolean latOk = p.getLat() >= SW.getLat() && p.getLat() < NW.getLat();
        boolean lonOk = p.getLon() >= SW.getLon() && p.getLon() < SE.getLon();

        boolean inIncludedSegments = false;
        for (GeoSegment includedSegment : includedSegments) {
            if (includedSegment.containsPoint(p)) {
                inIncludedSegments = true;
                break;
            }
        }
        return latOk && lonOk || inIncludedSegments;
    }

    @Override
    public String toString() {
        return "GeoCell(" + id + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoCell geoCell = (GeoCell) o;
        return id == geoCell.id && Objects.equals(NW, geoCell.NW) && Objects.equals(NE, geoCell.NE) && Objects.equals(SW, geoCell.SW) && Objects.equals(SE, geoCell.SE) && Objects.equals(includedSegments, geoCell.includedSegments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, NW, NE, SW, SE, includedSegments);
    }

    public enum CellType {
        TwoBorders, ThreeBordersEast, ThreeBordersNorth, FourBorders
    }
}
