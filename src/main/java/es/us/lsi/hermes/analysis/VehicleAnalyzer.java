package es.us.lsi.hermes.analysis;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import es.us.lsi.hermes.kafka.Kafka;
import es.us.lsi.hermes.smartDriver.VehicleLocation;
import es.us.lsi.hermes.util.Util;
import es.us.lsi.hermes.util.Constants;

import java.util.*;
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
        super("VehicleAnalyzer", true);
        this.kafkaVLConsumer = new KafkaConsumer<>(Kafka.getKafkaDataStorageConsumerProperties());
        this.kafkaAVProducer = new KafkaProducer<>(Kafka.getKafkaDataAnalyzerProperties());
        this.kafkaSVProducer = new KafkaProducer<>(Kafka.getKafkaDataStorageProducerProperties());
        this.pollTimeout = pollTimeout;
        this.oblivionTimeoutInMilliseconds = Long.parseLong(Kafka.getKafkaDataStorageConsumerProperties()
                .getProperty("vehicle.oblivion.timeout.s", "60")) * 1000;
        VehicleAnalyzer.analyzedVehicles = new HashMap<>();
        this.kafkaVLConsumer.subscribe(Collections.singletonList(Kafka.TOPIC_VEHICLE_LOCATION));
        this.gson = new Gson();
    }

    /**
     * The 'consumer' will wait 'pollTimeout' for messages from the topic 'VehicleLocations' to get every message
     * in real time.
     */
    @Override
    public void doWork() {
        ConsumerRecords<Long, String> records = kafkaVLConsumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - {0}: {1} [{2}] with offset {3}",
                    new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            // Get the data since the last poll and process it
            VehicleLocation vehicleLocation;
            try {
                vehicleLocation = new Gson().fromJson(record.value(), VehicleLocation.class);
                if (vehicleLocation == null) {
                    throw new JsonSyntaxException("Null vehicle");
                }
            } catch (JsonSyntaxException ex) {
                LOG.log(Level.SEVERE, "VehicleAnalyzer.doWork() - Invalid 'VehicleLocation': {0}", record.value());
                continue;
            }

            LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - VEHICLE LOCATION {0} with vehicle-id {1}",
                    new Object[]{vehicleLocation.getTimeStamp(), vehicleLocation.getVehicleId()});

            // A 'HashMap' will store the vehicles with the most updated information, in essence those who are moving
            Vehicle analyzedVehicle = analyzedVehicles.get(vehicleLocation.getVehicleId());

            // After getting searching for the vehicle with its sourceId, process and store in case it doesn't exists
            if (analyzedVehicle == null) {
                LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - New vehicle: id={0}", vehicleLocation.getVehicleId());
                analyzedVehicle = new Vehicle(vehicleLocation.getVehicleId());
                analyzedVehicles.put(vehicleLocation.getVehicleId(), analyzedVehicle);
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
       for (Iterator<Map.Entry<String, Vehicle>> it = analyzedVehicles.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Vehicle> entry = it.next();
            if (System.currentTimeMillis() - entry.getValue().getLastUpdate() > oblivionTimeoutInMilliseconds) {
                it.remove();
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
            LOG.log(Level.FINE, "AnalyzeVehicles.run() - Id of surrounding vehicles ({0} - {1})",
                    new Object[]{currentVehicle.getId(), otherVehicle.getId()});
            currentVehicle.getSurroundingVehicles().add(otherVehicle.getId());
            otherVehicle.getSurroundingVehicles().add(currentVehicle.getId());
        }
    }

    public void stopConsumer() {
        kafkaVLConsumer.close();
        shutdown();
    }
}
