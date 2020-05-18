import java.io.Serializable;

/**
 * A simple two-dimensional point.
 */
public class Point implements Serializable {

    public double lon, lat, distance;
    public Integer centriodId;

    public Point() {}

    public Point(double lon, double lat) {
        this.lon = lon;
        this.lat = lat;
        this.distance = Double.MAX_VALUE;
        this.centriodId = 0;
    }

    public Point add(Point other) {
        lon += other.lon;
        lat += other.lat;
        return this;
    }

    public Point div(long val) {
        lon /= val;
        lat /= val;
        return this;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((lon - other.lon) * (lon - other.lon) + (lat - other.lat) * (lat - other.lat));
    }

    public void clear() {
        lon = lat = 0.0;
    }

    @Override
    public String toString() {
        return lon + "," + lat;
    }
}
