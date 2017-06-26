package es.us.lsi.hermes.analysis;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import es.us.lsi.hermes.Kafka;
import es.us.lsi.hermes.util.Util;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.kafka.Event;
import es.us.lsi.hermes.kafka.ExtendedEvent;
import es.us.lsi.hermes.smartDriver.Location;
import es.us.lsi.hermes.util.Constants;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

    private final KafkaConsumer<Long, String> kafkaConsumer;
    private final KafkaProducer<Long, String> kafkaProducer;
    private final long pollTimeout;
    private final long oblivionTimeoutInMilliseconds;
    private static HashMap<String, Vehicle> analyzedVehicles;

    public VehicleAnalyzer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("VehicleAnalyzer", true);
        this.kafkaConsumer = new KafkaConsumer<>(Kafka.getKafkaDataStorageConsumerProperties());
        this.kafkaProducer = new KafkaProducer<>(Kafka.getKafkaDataAnalyzerProperties());
        this.pollTimeout = pollTimeout;
        this.oblivionTimeoutInMilliseconds = Long.parseLong(Kafka.getKafkaDataStorageConsumerProperties().getProperty("vehicle.oblivion.timeout.s", "60")) * 1000;
        analyzedVehicles = new HashMap<>();
        kafkaConsumer.subscribe(Collections.singletonList(Kafka.TOPIC_VEHICLE_LOCATION));
    }

    @Override
    public void doWork() {
        // The 'consumer' for each 'VehicleLocations' will poll every 'pollTimeout' milliseconds, to get all the data received by Kafka.
        ConsumerRecords<Long, String> records = kafkaConsumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - {0}: {1} [{2}] with offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            // Get the data since the last poll and process it
            ExtendedEvent event;
            try {
                event = new Gson().fromJson(record.value(), ExtendedEvent.class);
                if (event == null) {
                    LOG.log(Level.SEVERE, "VehicleAnalyzer.doWork() - Null event: {0}", record.value());
                    continue;
                }
            } catch (JsonSyntaxException ex) {
                LOG.log(Level.SEVERE, "VehicleAnalyzer.doWork() - Invalid 'VehicleLocation' event: {0}", record.value());
                continue;
            }

            LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - VEHICLE LOCATION {0} with event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

            // Get the vehicle location.
            Location vehicleLocation = Util.getVehicleLocationFromEvent(event);
            if (vehicleLocation == null) {
                continue;
            }

            // A 'HashMap' will store the vehicles with the most updated information, in essence those who are moving
            Vehicle analyzedVehicle = analyzedVehicles.get(event.getSourceId());

            // After getting searching for the vehicle with its sourceId, process and store in case it doesn't exists
            if (analyzedVehicle == null) {
                LOG.log(Level.FINE, "VehicleAnalyzer.doWork() - New vehicle: id={0}", event.getSourceId());
                analyzedVehicle = new Vehicle(event.getSourceId());
                analyzedVehicles.put(event.getSourceId(), analyzedVehicle);
            }

            analyzedVehicle.update(vehicleLocation);

            // TODO: Los surrounding!!!
//                // Add a record to the vehicle's location history
//                analyzedVehicle.addHistoricLocation(vehicleLocation.getTimeStamp(), vehicleLocation);
//                // Reset the "forget the vehicle if inactive" timeout
//                analyzedVehicle.resetOblivionTimeout();
//
//                // Notify the observer about data changes
//                // FIXME: Remove?       <- This triggers the surrounding analysis
//                observer.update();
        }

        for (Iterator<Map.Entry<String, Vehicle>> it = analyzedVehicles.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Vehicle> entry = it.next();
            if (System.currentTimeMillis() - entry.getValue().getLastUpdate() > oblivionTimeoutInMilliseconds) {
                it.remove();
            }
        }

        String json = new Gson().toJson(analyzedVehicles.values());
        ProducerRecord producerRecord = new ProducerRecord<>(Kafka.TOPIC_ACTIVE_VEHICLES, json);
        kafkaProducer.send(producerRecord);
    }

    public void stopConsumer() {
        kafkaConsumer.close();
        shutdown();
    }

    public static HashMap<String, Vehicle> getAnalyzedVehicles() {
        return analyzedVehicles;
    }
}
