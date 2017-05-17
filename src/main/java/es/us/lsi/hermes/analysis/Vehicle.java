package es.us.lsi.hermes.analysis;

import es.us.lsi.hermes.Main;
import es.us.lsi.hermes.smartDriver.Location;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Vehicle {

    private final String id;
    private int score;
    private final LinkedHashMap<String, Location> historicLocations;
    private final Set<String> surroundingVehicles;
    private Integer oblivionTimeout;

    /**
     * Constructor en el que se indicará el identificador del 'SmartDriver', el
     * máximo de ubicaciones pasadas que se analizarán y el número máximo de
     * otros 'SmartDrivers' que se tendrán en cuenta.
     *
     * @param id identificador del 'SmartDriver'
     * @param historySize Número máximo de ubicaciones que se tendrán en cuenta.
     */
    public Vehicle(String id, final Integer historySize) {
        this.id = id;
        this.score = 0;
        this.oblivionTimeout = Integer.parseInt(Main.getKafkaProperties().getProperty("vehicle.oblivion.timeout.s", "60"));
        this.historicLocations = new LinkedHashMap<String, Location>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Location> eldest) {
                return this.size() > historySize;
            }
        };
        this.surroundingVehicles = new HashSet<>();
    }

    public void decreaseOblivionTimeout() {
        oblivionTimeout = oblivionTimeout > 0 ? --oblivionTimeout : 0;
    }

    public Integer getOblivionTimeout() {
        return oblivionTimeout;
    }

    public void resetOblivionTimeout() {
        oblivionTimeout = Integer.parseInt(Main.getKafkaProperties().getProperty("vehicle.oblivion.timeout.s", "60"));
    }

    public void addHistoricLocation(String timeStamp, Location location) {
        historicLocations.put(timeStamp, location);
    }

    public Map.Entry<String, Location> getMostRecentHistoricLocationEntry() {
        if (!historicLocations.isEmpty()) {
            return historicLocations.entrySet().iterator().next();
        } else {
            return null;
        }
    }

    public void addSurroundingVehicle(String id) {
        surroundingVehicles.add(id);
    }

    public String getId() {
        return id;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public LinkedHashMap<String, Location> getHistoricLocations() {
        return historicLocations;
    }

    public List<Location> getHistoricLocationsList() {
        return new ArrayList<>(historicLocations.values());
    }

    public Set<String> getSurroundingVehicles() {
        return surroundingVehicles;
    }

    public synchronized List<SurroundingVehicle> getSurroundingVehiclesList() {
        List result = new ArrayList();

        for (String surroundingVehicleId : surroundingVehicles) {
            // FIX
            Vehicle v = Main.getAnalyzedVehicle(surroundingVehicleId);
            if (v != null) {
                result.add(new SurroundingVehicle(v));
            }
        }

        return result;
    }

    public class SurroundingVehicle {

        private String id;
        private int score;
        private Double latitude;
        private Double longitude;

        public SurroundingVehicle(Vehicle v) {
            this.id = v.getId();
            this.score = v.getScore();
            Map.Entry<String, Location> mrl = v.getMostRecentHistoricLocationEntry();
            this.latitude = mrl.getValue().getLatitude();
            this.longitude = mrl.getValue().getLongitude();
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public Double getLatitude() {
            return latitude;
        }

        public void setLatitude(Double latitude) {
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public void setLongitude(Double longitude) {
            this.longitude = longitude;
        }

    }
}
