package es.us.lsi.hermes.kafka.consumer;

import es.us.lsi.hermes.Main;
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
    private static final String TOPIC_DATA_SECTION = "DataSection";

    private final KafkaConsumer<Long, String> consumer;
    private final long pollTimeout;

    public DataSectionConsumer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("DataSectionConsumer", true);
        this.consumer = new KafkaConsumer<>(Main.getKafkaProperties());
        this.pollTimeout = pollTimeout;
    }

    @Override
    public void doWork() {
        System.out.println("DataSection Consumer");

        consumer.subscribe(Collections.singletonList(TOPIC_DATA_SECTION));
        ConsumerRecords<Long, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<Long, String> record : records) {
            LOG.log(Level.FINE, "DataSectionConsumer.doWork() - {0}: {1} [{2}] con offset {3}", new Object[]{record.topic(), Constants.dfISO8601.format(record.timestamp()), record.key(), record.offset()});

            Event[] events = Util.getEventsFromJson(record.value());
            if (events == null || events.length <= 0) {
                LOG.log(Level.SEVERE, "DataSectionConsumer.doWork() - Error al obtener los eventos de tipo 'Data Section' del JSON recibido: {0}", record.value());
                continue;
            }

            // Es un conjunto de eventos de tipo 'Data Section'.
            for (Event event : events) {
                LOG.log(Level.FINE, "DataSectionConsumer.doWork() - DATA SECTION {0} con event-id {1}", new Object[]{event.getTimestamp(), event.getEventId()});

//                    DataSection dataSection = Util.getDataSectionFromEvent(event);
//                    if (dataSection != null) {
//                        TODO: Envío de datos a A Coruña.
//                    }
            }
        }
    }
}
