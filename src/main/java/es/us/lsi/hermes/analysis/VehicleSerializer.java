package es.us.lsi.hermes.analysis;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import es.us.lsi.hermes.smartDriver.Location;
import java.lang.reflect.Type;
import java.util.ArrayList;

public class VehicleSerializer implements JsonSerializer<Vehicle> {

    private final boolean includeCollections;

    public VehicleSerializer() {
        this.includeCollections = true;
    }

    public VehicleSerializer(boolean includeCollections) {
        this.includeCollections = includeCollections;
    }

    @Override
    public JsonElement serialize(Vehicle vehicle, Type type, JsonSerializationContext jsc) {
        Gson gson = new Gson();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("id", vehicle.getId());
        jsonObject.addProperty("stress", vehicle.getStress());

        if (includeCollections) {
            Type historicLocationsListType = new TypeToken<ArrayList<Location>>() { }.getType();
            jsonObject.add("historicLocations", gson.toJsonTree(vehicle.getHistoricLocationsList(), historicLocationsListType));
            Type surroundingVehiclesListType = new TypeToken<ArrayList<Vehicle.SurroundingVehicle>>() { }.getType();
            jsonObject.add("surroundingVehicles", gson.toJsonTree(vehicle.getSurroundingVehiclesList(), surroundingVehiclesListType));
        } else {
            jsonObject.add("lastLocation", gson.toJsonTree(vehicle.getMostRecentHistoricLocationEntry().getValue(), Location.class));
        }

        return jsonObject;
    }
}
