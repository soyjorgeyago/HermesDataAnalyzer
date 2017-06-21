package es.us.lsi.hermes.analysis;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Vehicle implements Serializable {

    private final String id;
    private double latitude;
    private double longitude;
    private int speed;
    private int stress;
    private final Set<String> surroundingVehicles;
    private long lastUpdate;

    /**
     * Constructor en el que se indicará el identificador del 'SmartDriver'.
     *
     * @param id identificador del 'SmartDriver'
     */
    public Vehicle(String id) {
        this.id = id;
        this.stress = 0;
        this.speed = 0;
        this.latitude = 0.0d;
        this.longitude = 0.0d;
        this.surroundingVehicles = new HashSet<>();
        this.lastUpdate = System.currentTimeMillis();
    }

    public void addSurroundingVehicle(String id) {
        surroundingVehicles.add(id);
        lastUpdate = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    
    public int getStress() {
        return stress;
    }

    public void setStress(int stress) {
        this.stress = stress;
        lastUpdate = System.currentTimeMillis();
    }

    public Set<String> getSurroundingVehicles() {
        return surroundingVehicles;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }
}
