package es.us.lsi.hermes.kafka.consumer;

import es.us.lsi.hermes.ISmartDriverObserver;
import es.us.lsi.hermes.Kafka;
import es.us.lsi.hermes.util.Util;
import es.us.lsi.hermes.Main;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.smartDriver.Location;
import es.us.lsi.hermes.util.Constants;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ztreamy.Event;

public class VehicleLocationConsumer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(VehicleLocationConsumer.class.getName());

    private final KafkaConsumer<Long, String> consumer;
    private final long pollTimeout;
    private final ISmartDriverObserver observer;

    public VehicleLocationConsumer(long pollTimeout, ISmartDriverObserver observer) {
        // Podrá ser interrumpible.
        super("VehicleLocationConsumer", true);
        this.consumer = new KafkaConsumer<>(Kafka.getKafkaDataStorageProperties());
        this.pollTimeout = pollTimeout;
        this.observer = observer;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(Kafka.TOPIC_VEHICLE_LOCATION));

        // The 'consumer' for each 'Vehicle Locations' will poll every 'pollTimeout' milisegundos, to get all the data received by Kafka.
        ConsumerRecords<Long, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.INFO, "VehicleLocationConsumer.doWork() - {0}: {1} [{2}] with offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            // Get the data since the last poll and process it
            Event events[] = Util.getEventsFromJson(record.value());
            if (events == null || events.length <= 0) {
                LOG.log(Level.SEVERE, "VehicleLocationConsumer.doWork() - Error obtaining 'Vehicle Location' events from the JSON received: {0}", record.value());
                continue;
            }

            for (Event event : events) {
                LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - VEHICLE LOCATION {0} with event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

                // Get the vehicle location.
                Location vehicleLocation = Util.getVehicleLocationFromEvent(event);
                if (vehicleLocation == null)
                    continue;

                // A 'HashMap' will store the vehicles with the most updated information, in essence those who are moving
                Vehicle analyzedVehicle = Main.getAnalyzedVehicle(event.getSourceId());

                // After getting searching for the vehicle with its sourceId, process and store in case it doesn't exists
                if (analyzedVehicle == null) {
                    LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - New vehicle: id={0}", event.getSourceId());
                    analyzedVehicle = new Vehicle(event.getSourceId(), Integer.parseInt(Kafka.getKafkaDataStorageProperties()
                            .getProperty("vehicleLocation.historic.size", "10")));
                    Main.addAnalyzedVehicle(event.getSourceId(), analyzedVehicle);
                }

                // Add a record to the vehicle's location history
                analyzedVehicle.addHistoricLocation(vehicleLocation.getTimeStamp(), vehicleLocation);
                // Reset the "forget the vehicle if inactive" timeout
                analyzedVehicle.resetOblivionTimeout();

                // Notify the observer about data changes
                // FIXME: Remove?       <- This triggers the surrounding analysis
                observer.update();
            }
        }

        // TODO: Test the Consumer without a working Kafka server
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }
}
