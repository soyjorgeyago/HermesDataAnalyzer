package es.us.lsi.hermes;

import es.us.lsi.hermes.analysis.VehicleAnalyzer;
import es.us.lsi.hermes.kafka.Kafka;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        LOG.log(Level.INFO, "main() - Cargar la configuración");

        long pollTimeout = Long.parseLong(Kafka.getKafkaDataStorageConsumerProperties().getProperty("consumer.poll.timeout.ms", "5000"));
        LOG.log(Level.INFO, "main() - Los \"consumers\" de Kafka esperaran nuevos datos {0} milisegundos como maximo", pollTimeout);

        LOG.log(Level.INFO, "main() - Inicialización del analizador de 'VehicleLocation' de Kafka");
        VehicleAnalyzer vehicleAnalyzer = new VehicleAnalyzer(pollTimeout);
        vehicleAnalyzer.start();
    }
}
