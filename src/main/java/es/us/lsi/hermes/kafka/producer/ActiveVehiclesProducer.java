package es.us.lsi.hermes.kafka.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import es.us.lsi.hermes.Kafka;
import es.us.lsi.hermes.Main;
import es.us.lsi.hermes.analysis.Vehicle;
import es.us.lsi.hermes.analysis.VehicleSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveVehiclesProducer extends Thread {

    private static final Logger LOG = Logger.getLogger(ActiveVehiclesProducer.class.getName());
    private static final AtomicLong KAFKA_RECORD_ID = new AtomicLong();
    private final Gson gson;

    public ActiveVehiclesProducer() {
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Vehicle.class, new VehicleSerializer(false));
        gson = gsonBuilder.create();
    }

    @Override
    public void run() {
        KafkaProducer<Long, String> producer = new KafkaProducer<>(Kafka.getKafkaDataAnalyzerProperties());

        // Send all active vehicles periodically
        String json = gson.toJson(Main.getAnalyzedVehicles());
        long id = KAFKA_RECORD_ID.getAndIncrement();
        LOG.log(Level.FINE, "run() - Topic: " + Kafka.TOPIC_ACTIVE_VEHICLES + " for all the active vehicles");
        producer.send(new ProducerRecord<>(Kafka.TOPIC_ACTIVE_VEHICLES, id, json), new KafkaCallBack(System.currentTimeMillis(), id));
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
