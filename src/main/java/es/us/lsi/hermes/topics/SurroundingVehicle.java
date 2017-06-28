package es.us.lsi.hermes.topics;

import es.us.lsi.hermes.analysis.Vehicle;
import java.io.Serializable;
import java.util.Set;

public class SurroundingVehicle implements Serializable {

    private final String id;
    private final Set<String> surroundingVehicles;

    public SurroundingVehicle(Vehicle vehicle) {
        this.id = vehicle.getId();
        this.surroundingVehicles = vehicle.getSurroundingVehicles();
    }

    public String getId() {
        return id;
    }

    public Set<String> getSurroundingVehicles() {
        return surroundingVehicles;
    }
}