package es.us.lsi.hermes.kafka.consumer;

import es.us.lsi.hermes.Kafka;
import es.us.lsi.hermes.util.Constants;
import es.us.lsi.hermes.util.Util;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ztreamy.Event;

public class DataSectionConsumer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(DataSectionConsumer.class.getName());

    private final KafkaConsumer<Long, String> kafkaConsumer;
    private final long pollTimeout;

    public DataSectionConsumer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("DataSectionConsumer", true);
        this.kafkaConsumer = new KafkaConsumer<>(Kafka.getKafkaDataStorageProperties());
        this.pollTimeout = pollTimeout;
        kafkaConsumer.subscribe(Collections.singletonList(Kafka.TOPIC_DATA_SECTION));
    }

    @Override
    public void doWork() {
        ConsumerRecords<Long, String> records = kafkaConsumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "DataSectionConsumer.doWork() - {0}: {1} [{2}] con offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            Event[] events = Util.getEventsFromJson(record.value());
            if (events == null || events.length <= 0) {
                LOG.log(Level.SEVERE, "DataSectionConsumer.doWork() - Error al obtener los eventos de tipo 'DataSection' del JSON recibido: {0}", record.value());
                continue;
            }

            // Es un conjunto de eventos de tipo 'DataSection'.
            for (Event event : events) {
                LOG.log(Level.FINE, "DataSectionConsumer.doWork() - 'DataSection' {0} con event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

//                    DataSection dataSection = Util.getDataSectionFromEvent(event);
//                    if (dataSection != null) {
//                        TODO: Envío de datos a A Coruña.
//                    }
            }
        }
    }
    
    public void stopConsumer() {
        kafkaConsumer.close();
        shutdown();
    }
}
