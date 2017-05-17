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

    @Override
    public JsonElement serialize(Vehicle vehicle, Type type, JsonSerializationContext jsc) {
        Gson gson = new Gson();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("id", vehicle.getId());
        jsonObject.addProperty("score", vehicle.getScore());
        Type historicLocationsListType = new TypeToken<ArrayList<Location>>() { }.getType();
        jsonObject.add("historicLocations", gson.toJsonTree(vehicle.getHistoricLocationsList(), historicLocationsListType));
        Type surroundingVehiclesListType = new TypeToken<ArrayList<Vehicle.SurroundingVehicle>>() { }.getType();
        jsonObject.add("surroundingVehicles", gson.toJsonTree(vehicle.getSurroundingVehiclesList(), surroundingVehiclesListType));
        return jsonObject;
    }

}
