package es.us.lsi.hermes.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {

    private static final Logger LOG = Logger.getLogger(Util.class.getName());
    
    public static double distance(double lat1, double lng1, double lat2, double lng2) {
        double p = 0.017453292519943295;
        double a = 0.5 - Math.cos((lat2 - lat1) * p) / 2
                + Math.cos(lat1 * p) * Math.cos(lat2 * p)
                * (1 - Math.cos((lng2 - lng1) * p)) / 2;

        return 12742000 * Math.asin(Math.sqrt(a));
    }

    /**
     * Implementación de la Fórmula de Haversine.
     * https://es.wikipedia.org/wiki/Fórmula_del_Haversine
     *
     * @param lat1 Latitud inicial.
     * @param lng1 Longitud inicial.
     * @param lat2 Latitud final.
     * @param lng2 Longitud final.
     * @return Distancia en metros entre los 2 puntos.
     */
    public static double distanceHaversine(double lat1, double lng1, double lat2, double lng2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double sindLat = Math.sin(dLat / 2);
        double sindLng = Math.sin(dLng / 2);
        double a = Math.pow(sindLat, 2) + Math.pow(sindLng, 2)
                * Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        // El radio de la Tierra es, aproximadamente, 6.371 Km, es decir, 6.371.000 metros.

        return 6371000.0d * c;
    }

    /**
     * Method for loading a properties file.
     *
     * @param propertiesFileName Name of the properties file.
     * @return Loaded properties file.
     */
    public static Properties initProperties(String propertiesFileName) {
        Properties result = new Properties();

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream(propertiesFileName);
            result.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "initProperties() - Error loading properties file: " + propertiesFileName, ex);
        } catch (NullPointerException ex) {
            LOG.log(Level.SEVERE, "initProperties() - File \'{0}\' not found", propertiesFileName);
        }

        return result;
    }
}
