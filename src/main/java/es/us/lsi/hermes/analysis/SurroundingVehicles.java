package es.us.lsi.hermes.analysis;

import com.google.gson.Gson;
import es.us.lsi.hermes.Kafka;
import es.us.lsi.hermes.analysis.Vehicle;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SurroundingVehicles extends Thread {

    private static final Logger LOG = Logger.getLogger(SurroundingVehicles.class.getName());
    private static final AtomicLong KAFKA_RECORD_ID = new AtomicLong();
    private final Collection<Vehicle> vehicles;

    public SurroundingVehicles(Collection<Vehicle> vehicles) {
        this.vehicles = vehicles;
    }

    @Override
    public void run() {
        KafkaProducer<Long, String> producer = new KafkaProducer<>(Kafka.getKafkaDataStorageProducerProperties());

        // Analizamos cuáles son los vehículos cercanos entre sí y lo publicamos.
        for (Vehicle v : vehicles) {
            String json = new Gson().toJson(v);
            long id = KAFKA_RECORD_ID.getAndIncrement();
            LOG.log(Level.FINE, "run() - Topic: " + Kafka.TOPIC_SURROUNDING_VEHICLES + " for the Vehicle with id: {0}", v.getId());
            producer.send(new ProducerRecord<>(Kafka.TOPIC_SURROUNDING_VEHICLES, id, json),
                    new KafkaCallBack(System.currentTimeMillis(), id));
        }
    }

    class KafkaCallBack implements Callback {

        private final long startTime;
        private final long key;

        KafkaCallBack(long startTime, long key) {
            this.startTime = startTime;
            this.key = key;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                LOG.log(Level.FINE, "onCompletion() - Mensaje enviado correctamente a Kafka\n - Key: {0}\n - Partición: {1}\n - Offset: {2}\n - Tiempo transcurrido: {3} ms", new Object[]{key, metadata.partition(), metadata.offset(), elapsedTime});
            } else {
                LOG.log(Level.SEVERE, "onCompletion() - No se ha podido enviar a Kafka", exception);
            }
        }
    }
}
