import java.io.Serializable;

public class Data implements Serializable {

    public String radio;
    public int cell;
    public Long mcc, net, area, range, samples, changeable, averageSignal, created, updated, unit;
    public double lon, lat;

    public Data() {}

    public Data(String radio, Long mcc, Long net, Long area, int cell, Long unit, double lon, double lat,
                  Long range, Long samples, Long changeable, Long created, Long updated, Long averageSignal) {
        this.radio = radio;
        this.mcc = mcc;
        this.net = net;
        this.area = area;
        this.cell = cell;
        this.unit = unit;
        this.lon = lon;
        this.lat = lat;
        this.range = range;
        this.samples = samples;
        this.changeable = changeable;
        this.created = created;
        this.updated = updated;
        this.averageSignal = averageSignal;
    }

    @Override
    public String toString() {
        return "Data{" +
                "radio='" + radio + '\'' +
                ", mcc=" + mcc +
                ", net=" + net +
                ", area=" + area +
                ", cell=" + cell +
                ", range=" + range +
                ", samples=" + samples +
                ", changeable=" + changeable +
                ", averageSignal=" + averageSignal +
                ", created=" + created +
                ", updated=" + updated +
                ", unit=" + unit +
                ", lon=" + lon +
                ", lat=" + lat +
                '}';
    }
}