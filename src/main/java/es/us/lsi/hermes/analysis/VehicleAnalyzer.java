package es.us.lsi.hermes.analysis;

import com.google.gson.Gson;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.util.Util;
import es.us.lsi.hermes.kafka.Event;
import es.us.lsi.hermes.smartDriver.Location;
import es.us.lsi.hermes.util.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class VehicleAnalyzer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(VehicleAnalyzer.class.getName());

    private final KafkaConsumer<Long, String> kafkaVLConsumer;
    private final KafkaProducer<Long, String> kafkaAVProducer,  kafkaSVProducer;
    private final long pollTimeout, oblivionTimeoutInMilliseconds;
    private static HashMap<String, Vehicle> analyzedVehicles;
    private final Gson gson;

    public VehicleAnalyzer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("VehicleLocationConsumer", true);
        this.kafkaVLConsumer = new KafkaConsumer<>(Kafka.getKafkaDataStorageConsumerProperties());
        this.kafkaAVProducer = new KafkaProducer<>(Kafka.getKafkaDataAnalyzerProperties());
        this.kafkaSVProducer = new KafkaProducer<>(Kafka.getKafkaDataStorageProducerProperties());
        this.pollTimeout = pollTimeout;
        this.oblivionTimeoutInMilliseconds = Long.parseLong(Kafka.getKafkaDataStorageConsumerProperties().getProperty("vehicle.oblivion.timeout.s", "60")) * 1000;
        VehicleAnalyzer.analyzedVehicles = new HashMap<>();
        this.kafkaVLConsumer.subscribe(Collections.singletonList(Kafka.TOPIC_VEHICLE_LOCATION));
        this.gson = new Gson();
    }

    @Override
    public void doWork() {
        // The 'consumer' for each 'VehicleLocations' will poll every 'pollTimeout' milliseconds, to get all the data received by Kafka.
        ConsumerRecords<Long, String> records = kafkaVLConsumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - {0}: {1} [{2}] with offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            // Get the data since the last poll and process it
            Event event = gson.fromJson(record.value(), Event.class);
            if (event == null) {
                LOG.log(Level.SEVERE, "VehicleLocationConsumer.doWork() - Error obtaining 'VehicleLocation' events from the JSON received: {0}", record.value());
                continue;
            }

            LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - VEHICLE LOCATION {0} with event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

            // Get the vehicle location.
            Location vehicleLocation = Util.getVehicleLocationFromEvent(event);
            if (vehicleLocation == null) {
                continue;
            }

            // A 'HashMap' will store the vehicles with the most updated information, in essence those who are moving
            Vehicle analyzedVehicle = analyzedVehicles.get(event.getSourceId());

            // After getting searching for the vehicle with its sourceId, process and store in case it doesn't exists
            if (analyzedVehicle == null) {
                LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - New vehicle: id={0}", event.getSourceId());
                analyzedVehicle = new Vehicle(event.getSourceId());
                analyzedVehicles.put(event.getSourceId(), analyzedVehicle);
            }

            // Update the data related to the vehicle's new location and also the active vehicles in the system
            analyzedVehicle.update(vehicleLocation);

            updateAnalysedVehiclesSurroundings(analyzedVehicle);

            // Build and send the Json with the vehicle information
            String json = gson.toJson(analyzedVehicle);
            kafkaSVProducer.send(new ProducerRecord<Long, String>(Kafka.TOPIC_SURROUNDING_VEHICLES, json));
        }

        // TODO - Review - In case of performance issues (CPU) <= Move to top
        // Remove inactive vehicles
        long currentTimeMillis = System.currentTimeMillis();
        for(Vehicle vehicle : analyzedVehicles.values()){
            if (currentTimeMillis - vehicle.getLastUpdate() > oblivionTimeoutInMilliseconds) {
                analyzedVehicles.remove(vehicle.getId());
            }
        }

        // Publish the all Active Vehicles available right now
        String json = gson.toJson(analyzedVehicles.values());
        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(Kafka.TOPIC_ACTIVE_VEHICLES, json);
        kafkaAVProducer.send(producerRecord);
    }

    private void updateAnalysedVehiclesSurroundings(Vehicle currentVehicle) {

        Collection<String> oldNeighbours = new ArrayList<>();
        // Analizamos los vehículos que ya están en su radio de influencia, por si hay que quitar alguno.
        for (String id : currentVehicle.getSurroundingVehicles()) {

            Vehicle surroundingVehicle = analyzedVehicles.get(id);

            // Calculamos la distancia con el método rápido.
            double distance = Util.distance(currentVehicle.getLatitude(), currentVehicle.getLongitude(),
                    surroundingVehicle.getLatitude(), surroundingVehicle.getLongitude());

            //If the surroundingVehicle is nearby and is an active vehicle
            if (distance <= Constants.SURROUNDING_DIAMETER && analyzedVehicles.containsKey(id)) {
                continue;
            }

            LOG.log(Level.FINE, "AnalyzeVehicles.run() - Los vehículos han dejado de influirse ({0} - {1})",
                    new Object[]{currentVehicle.getId(), surroundingVehicle.getId()});

            // Add the vehicle to the list of all neighbours, soon to be removed
            oldNeighbours.add(surroundingVehicle.getId());
            // Del mismo modo, también eliminamos el identificador del vehículo actual del conjunto del otro vehículo.
            surroundingVehicle.getSurroundingVehicles().remove(currentVehicle.getId());

        }
        // Remove all the old neighbours
        currentVehicle.getSurroundingVehicles().removeAll(oldNeighbours);

        for (Vehicle otherVehicle : analyzedVehicles.values()) {
            // Analizamos su relación con los otros vehículos que no están en su conjunto de vehículos cercanos.

            // If it's the same Vehicle or is a Surrounding one,
            if (currentVehicle.getId().equals(otherVehicle.getId())
                    || currentVehicle.getSurroundingVehicles().contains(otherVehicle.getId())) {
                continue;
            }

            // Calculamos la distancia con el método rápido.
            double distance = Util.distance(currentVehicle.getLatitude(), currentVehicle.getLongitude(),
                    otherVehicle.getLatitude(), otherVehicle.getLongitude());
            // Check if it meets our proximity requirements
            if (distance > Constants.SURROUNDING_DIAMETER) {
                continue;
            }

            // Están en su zona de influencia. Los 2 vehículos se influyen.
            LOG.log(Level.FINE, "AnalyzeVehicles.run() - Identificadores de los vehículos que se influyen ({0} - {1})", new Object[]{currentVehicle.getId(), otherVehicle.getId()});
            currentVehicle.getSurroundingVehicles().add(otherVehicle.getId());
            otherVehicle.getSurroundingVehicles().add(currentVehicle.getId());
        }
    }

    public void stopConsumer() {
        kafkaVLConsumer.close();
        shutdown();
    }
}
