package com.diagiac.flink.query3.model;

import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Model/Entity class for query3.
 * It represents a geographic cell in the 4x4 grid.
 * It is identified by an id and has
 * - 4 geographic points (latitude and longitude)
 * - a list of included segments.
 *
 * Our convention is that the SUD and WEST segments are included in the cell.
 * while the NORTH segment belongs to the upper cell (if present, otherwise belongs to this cell)
 * and the EAST segment belong to the cell to the right (if present, otherwise belongs to this cell)
 *
 * Furthermore, we choose not to include the NW and SE point to the cell, they also belongs respectively
 * to the UPPER and RIGHT. If there is no upper cell, the NW point is included in this cell
 * If there is no right cell, the SE point belongs to this cell.
 */
@Data
public class GeoCell {
    private int id;
    private GeoPoint NW; // NW ----- NE
    private GeoPoint NE; // |        |
    private GeoPoint SW; // |        |
    private GeoPoint SE; // SW ----- SE

    private List<GeoSegment> includedSegments;

    /**
     * This constructor for the cell only needs two geo points for simplicity (must be SW and NE).
     * The other two points are computed automatically.
     * @param id of cell.
     * @param southWest the SW GeoPoint (lat and long)
     * @param northEast the NE GeoPoint (lat and long)
     * @param cellType the cell type. It can be TwoBorders, ThreeBordersEast, ThreeBordersNorth or FourBorders.
     *                 See {@link CellType} for more detail.
     */
    public GeoCell(int id, GeoPoint southWest, GeoPoint northEast, CellType cellType) {
        this.id = id;
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

    /**
     * Only used to initialize an empty GeoCell.
     * Use the other constructor to initialize a correct GeoCell.
     * @param i id of the cell
     */
    public GeoCell(int i) {
        this.id = i;
    }

    /**
     * This method checks if a GeoPoint p is inside this cell.
     * Checks for borders segments and corners are also included.
     *
     * @param p the GeoPoint (lat and long)
     * @return true if the point is inside
     */
    public boolean containsGeoPoint(GeoPoint p) {
        // check if the point is inside the cell. The less than (<) is present because
        // by hypothesis we consider the NW and SE points not included
        // but the next check verifies if those points are included
        boolean latOk = p.getLat() >= SW.getLat() && p.getLat() < NW.getLat();
        boolean lonOk = p.getLon() >= SW.getLon() && p.getLon() < SE.getLon();

        // check if the point is inside the border (or the point is an included corner)
        boolean inIncludedSegments = false;
        for (GeoSegment includedSegment : includedSegments) {
            if (includedSegment.containsPoint(p)) {
                inIncludedSegments = true; // if it is included in at least one segment, the check ends
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

    /**
     * TwoBorders: all the cell in the 3x3 grid that is in the bottom left corner of the 4x4 grid have two borders included (SOUTH and WEST)
     * ThreeBordersEast: the three EXTREME EAST cell in the 4x4 grid have at least three corners included (SOUTH, WEST and EAST, but not NORTH)
     * ThreeBordersNorth: the three EXTREME NORTH cell in the 4x4 grid have at least three corners included (SOUTH, WEST and NORTH, but not EAST)
     * FourBorders: the only cell with four border is the one in the UPPER RIGHT corner.
     *
     * Follows the disposition of cell with the number representing the # of borders for the cell in the grid.
     *
     *                                                    3 3 3 4
     *                                                    2 2 2 3
     *                                                    2 2 2 3
     *                                                    2 2 2 3
     */
    public enum CellType {
        TwoBorders, ThreeBordersEast, ThreeBordersNorth, FourBorders
    }
}
