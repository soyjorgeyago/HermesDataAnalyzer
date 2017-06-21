package es.us.lsi.hermes;

import es.us.lsi.hermes.util.Util;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Kafka {

    private static final Logger LOG = Logger.getLogger(Kafka.class.getName());

    public static final String TOPIC_VEHICLE_LOCATION = "VehicleLocation";
    public static final String TOPIC_DATA_SECTION = "DataSection";
    public static final String TOPIC_SURROUNDING_VEHICLES = "SurroundingVehicles";
    public static final String TOPIC_ACTIVE_VEHICLES = "ActiveVehicles";

    private static final Properties KAFKA_DATA_STORAGE_CONSUMER_PROPERTIES;
    private static final Properties KAFKA_DATA_STORAGE_PRODUCER_PROPERTIES;
    private static final Properties KAFKA_DATA_ANALYZER_PROPERTIES;

    static {
        LOG.log(Level.INFO, "Kafka() - Kafka init.");

        KAFKA_DATA_STORAGE_CONSUMER_PROPERTIES = Util.initProperties("DataStorageConsumer.properties");
        KAFKA_DATA_STORAGE_PRODUCER_PROPERTIES = Util.initProperties("DataStorageProducer.properties");
        KAFKA_DATA_ANALYZER_PROPERTIES = Util.initProperties("DataAnalyzer.properties");
    }

    public static Properties getKafkaDataStorageConsumerProperties() {
        return KAFKA_DATA_STORAGE_CONSUMER_PROPERTIES;
    }
    
    public static Properties getKafkaDataStorageProducerProperties() {
        return KAFKA_DATA_STORAGE_PRODUCER_PROPERTIES;
    }

    public static Properties getKafkaDataAnalyzerProperties() {
        return KAFKA_DATA_ANALYZER_PROPERTIES;
    }
}
