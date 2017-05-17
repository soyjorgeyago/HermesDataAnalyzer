package es.us.lsi.hermes.kafka.consumer;

import es.us.lsi.hermes.ISmartDriverObserver;
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
    public static final String TOPIC_VEHICLE_LOCATION = "VehicleLocation";

    private final KafkaConsumer<Long, String> consumer;
    private final long pollTimeout;
    private final ISmartDriverObserver observer;

    public VehicleLocationConsumer(long pollTimeout, ISmartDriverObserver observer) {
        // Podrá ser interrumpible.
        super("VehicleLocationConsumer", true);
        this.consumer = new KafkaConsumer<>(Main.getKafkaProperties());
        this.pollTimeout = pollTimeout;
        this.observer = observer;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(TOPIC_VEHICLE_LOCATION));
        // El 'consumer' para los 'Vehicle Locations' hará consultas cada 'pollTimeout' milisegundos, para traerse los datos que hayan llegado a Kafka.
        ConsumerRecords<Long, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - {0}: {1} [{2}] con offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            // Obtenemos el conjunto de eventos que se hayan recibido desde la última consulta.
            Event events[] = Util.getEventsFromJson(record.value());
            if (events != null && events.length > 0) {
                for (Event event : events) {
                    LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - VEHICLE LOCATION {0} con event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

                    // Obtenemos la ubicación del vehículo.
                    Location vehicleLocation = Util.getVehicleLocationFromEvent(event);
                    if (vehicleLocation != null) {
                        // Tendremos un 'HashMap' con los vehículos de los que se tenga información más actualizada, es decir, sólo los que se estén moviendo.
                        // Vemos si es un vehículo que ya tenemos en análisis. Obtenemos el 'sourceId' del evento, que será el indicador unívoco del vehículo.
                        Vehicle analyzedVehicle = Main.getAnalyzedVehicle(event.getSourceId());
                        if (analyzedVehicle == null) {
                            // Es un vehículo nuevo.
                            LOG.log(Level.FINE, "VehicleLocationConsumer.doWork() - Nuevo vehículo: id={0}", event.getSourceId());
                            analyzedVehicle = new Vehicle(event.getSourceId(),
                                    Integer.parseInt(Main.getKafkaProperties().getProperty("vehicleLocation.historic.size", "10")));
                            Main.addAnalyzedVehicle(event.getSourceId(), analyzedVehicle);
                        }
                        // Le añadimos un registro más a su histórico de localizaciones.
                        analyzedVehicle.addHistoricLocation(vehicleLocation.getTimeStamp(), vehicleLocation);
                        // Iniciamos de nuevo el contador de descarte del vehículo.
                        analyzedVehicle.resetOblivionTimeout();
                        
                        // Notificamos al observador que se ha habido actualización de datos.
                        // FIXME: ¿Quitar?
                        observer.update();
                    }
                }
            } else {
                LOG.log(Level.SEVERE, "VehicleLocationConsumer.doWork() - Error al obtener los eventos de tipo 'Vehicle Location' del JSON recibido: {0}", record.value());
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }
}
