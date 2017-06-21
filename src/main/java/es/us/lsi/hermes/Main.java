package es.us.lsi.hermes;

import es.us.lsi.hermes.analysis.VehicleAnalyzer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        LOG.log(Level.INFO, "main() - Cargar la configuración");

        long pollTimeout = Long.parseLong(Kafka.getKafkaDataStorageConsumerProperties().getProperty("consumer.poll.timeout.ms", "5000"));
        LOG.log(Level.INFO, "main() - Los ''consumers'' de Kafka harán sondeos para ver si hay nuevos datos cada {0} milisegundos", pollTimeout);

        LOG.log(Level.INFO, "main() - Inicialización del consumidor de 'VehicleLocation' de Kafka");
        VehicleAnalyzer vehicleLocationConsumer = new VehicleAnalyzer(pollTimeout);
        vehicleLocationConsumer.start();
    }

//    /**
//     * Clase para analizar sólo los vehículos que tengan actividad y poder
//     * ahorrar recursos. Cuando se reciben datos de un vehículo, éste pasará al
//     * sistema y será analizado para estudiar las interacciones con el resto. Si
//     * pasa el tiempo establecido en el parámetro 'vehicle.oblivion.timeout.s',
//     * éste se descartará.
//     */
//    static class OblivionRunnable implements Runnable {
//
//        @Override
//        public void run() {
//            for (Iterator<Map.Entry<String, Vehicle>> it = ANALYZED_VEHICLES.entrySet().iterator(); it.hasNext();) {
//                Map.Entry<String, Vehicle> entry = it.next();
//                LOG.log(Level.FINE, "OblivionRunnable.run() - Al vehículo con id: {0} le quedan {1} segundos antes de ser olvidado.", new Object[]{entry.getValue().getId(), entry.getValue().getOblivionTimeout()});
//                // Cada vez que es llamado el método, se resta una unidad al tiempo para olvidar los vehículos.
//                entry.getValue().decreaseOblivionTimeout();
//
//                // Comprobamos el 'timeout' de cada vehículo, por si debemos olvidarnos de éste por no tener actividad.
//                if (entry.getValue().getOblivionTimeout() <= 0) {
//                    LOG.log(Level.FINE, "OblivionRunnable.run() - Se elimina el vehículo con id: {0} por falta de actividad.", entry.getValue().getId());
//                    it.remove();
//                }
//            }
//
//            // Si hay vehículos en análisis, lanzamos el 'producer' para registrar 'streams' que puedan ser consumidos.
//            if (!ANALYZED_VEHICLES.isEmpty()) {
//                LOG.log(Level.FINE, "OblivionRunnable.run() - Hay vehículos en análisis, se producen 'streams' para que puedan ser consumidos por SmartDriver.");
//                SurroundingVehiclesProducer surroundingVehiclesProducer = new SurroundingVehiclesProducer(ANALYZED_VEHICLES.values());
//                surroundingVehiclesProducer.start();
//            }
//        }
//    }
//
//    /**
//     * Clase para el análisis de los vehículos en relación al resto. Se valorará
//     * la distancia de unos a otros y la puntuación obtenida en base a los
//     * parámetros contemplados en SmartDriver: PKE, velocidad máxima y
//     * desviación típica de la velocidad.
//     */
//    static class AnalyzeVehicles implements Runnable {
//
//        // TODO: Ver si se puede transmitir el radio desde SmartDriver (simulador o instancias reales) para que sea configurable para cada uno.
//        private final double RADIUS = 100; // 100 metros.
//        private final double DIAMETER = 2 * RADIUS; // Si una distancia entre 2 vehículos es menor que 2 radios, se influenciarán.
//
//        @Override
//        public void run() {
//
//            for (Vehicle currentVehicle : ANALYZED_VEHICLES.values()) {
//
//                // Obtenemos la posición más reciente del vehículo.
//                Map.Entry<String, Location> currentVehicleEntry = currentVehicle.getMostRecentHistoricLocationEntry();
//
//                if (currentVehicleEntry == null) {
//                    continue;
//                }
//
//                Location currentVehicleLastPosition = currentVehicleEntry.getValue();
//
//                // Analizamos los vehículos que ya están en su radio de influencia, por si hay que quitar alguno.
//                for (String id : currentVehicle.getSurroundingVehicles()) {
//                    Vehicle surroundingVehicle = ANALYZED_VEHICLES.get(id);
//
//                    // Obtenemos la posición más reciente del vehículo circundante, si la tuviera.
//                    Map.Entry<String, Location> surroundingVehicleEntry = surroundingVehicle.getMostRecentHistoricLocationEntry();
//                    if (surroundingVehicleEntry == null) {
//                        continue;
//                    }
//
//                    Location surroundingVehicleLastPosition = surroundingVehicleEntry.getValue();
//                    // Calculamos la distancia con el método rápido.
//                    double distance = Util.distance(currentVehicleLastPosition.getLatitude(),
//                            currentVehicleLastPosition.getLongitude(),
//                            surroundingVehicleLastPosition.getLatitude(),
//                            surroundingVehicleLastPosition.getLongitude());
//                    if (distance <= DIAMETER) {
//                        continue;
//                    }
//
//                    LOG.log(Level.FINE, "AnalyzeVehicles.run() - Los vehículos han dejado de influirse ({0} - {1})",
//                            new Object[]{currentVehicle.getId(), surroundingVehicle.getId()});
//
//                    // Eliminamos el identificador del vehículo, del conjunto de vehículos que le influyen.
//                    currentVehicle.getSurroundingVehicles().remove(surroundingVehicle.getId());
//                    // Del mismo modo, también eliminamos el identificador del vehículo actual del conjunto del otro vehículo.
//                    surroundingVehicle.getSurroundingVehicles().remove(currentVehicle.getId());
//                }
//
//                for (Vehicle otherVehicle : ANALYZED_VEHICLES.values()) {
//                    // Analizamos su relación con los otros vehículos que no están en su conjunto de vehículos cercanos.
//
//                    // TODO RDL: Tweaked, review against commit 408389c
//                    if (currentVehicle.getId().equals(otherVehicle.getId())
//                            || currentVehicle.getSurroundingVehicles().contains(otherVehicle.getId())) {
//                        continue;
//                    }
//
//                    // Obtenemos la ubicación más reciente del vehículo actual.
//                    currentVehicleLastPosition = currentVehicleEntry.getValue();
//
//                    // Obtenemos la ubicación más reciente del otro vehículo.
//                    Map.Entry<String, Location> otherVehicleEntry = currentVehicle.getMostRecentHistoricLocationEntry();
//                    if (otherVehicleEntry == null) {
//                        continue;
//                    }
//
//                    Location otherVehicleLastPosition = otherVehicleEntry.getValue();
//
//                    // Calculamos la distancia con el método rápido.
//                    double distance = Util.distance(currentVehicleLastPosition.getLatitude(),
//                            currentVehicleLastPosition.getLongitude(),
//                            otherVehicleLastPosition.getLatitude(),
//                            otherVehicleLastPosition.getLongitude());
//                    // Check if it meets our proximity requirements
//                    if (distance > DIAMETER) {
//                        continue;
//                    }
//
//                    // Están en su zona de influencia. Los 2 vehículos se influyen.
//                    LOG.log(Level.FINE, "AnalyzeVehicles.run() - Identificadores de los vehículos que se influyen ({0} - {1})", new Object[]{currentVehicle.getId(), otherVehicle.getId()});
//                    currentVehicle.addSurroundingVehicle(otherVehicle.getId());
//                    otherVehicle.addSurroundingVehicle(currentVehicle.getId());
//                }
//            }
//        }
//    }
}
