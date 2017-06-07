package es.us.lsi.hermes.util;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import es.us.lsi.hermes.smartDriver.DataSection;
import es.us.lsi.hermes.smartDriver.Location;
import java.util.logging.Level;
import java.util.logging.Logger;
import ztreamy.Event;

public class Util {

    private static final Logger LOG = Logger.getLogger(Util.class.getName());

    /**
     * Método para obtener los eventos de Ztreamy recibidos. Como pueden llegar
     * eventos en forma de 'array' o de uno en uno, intentamos, en primer lugar,
     * la obtención como un único evento, que será el más habitual, y si
     * fallase, lo intentamos como 'array' de eventos.
     *
     * @param json JSON con el/los eventos recibidos.
     * @return Array con el/los evento/s obtenido/s del JSON.
     */
    public static Event[] getEventsFromJson(String json) {
        Event events[] = null;
        Gson gson = new Gson();

        // Comprobamos si llega un solo evento
        try {
            Event event = gson.fromJson(json, Event.class);
            events = new Event[]{event};
        } catch (JsonSyntaxException ex) {
            LOG.log(Level.SEVERE, "getEventsFromJson() - Error al intentar obtener un evento desde el JSON", ex.getMessage());
        }

        if (events == null) {
            try {
                events = gson.fromJson(json, Event[].class);
            } catch (JsonSyntaxException ex) {
                LOG.log(Level.SEVERE, "getEventsFromJson() - Error al intentar obtener un array de eventos desde el JSON", ex.getMessage());
            }
        }

        return events;
    }

    public static Location getVehicleLocationFromEvent(Event event) {
        Gson gson = new Gson();

        try {
            return gson.fromJson(gson.toJson(event.getBody().get("Location")), Location.class);
        } catch (JsonSyntaxException ex) {
            LOG.log(Level.SEVERE, "getVehicleLocationFromEvent() - Error al intentar obtener un 'VehicleLocation' de un evento", ex.getMessage());
        }

        return null;
    }
    
    public static DataSection getDataSectionFromEvent(Event event) {
        Gson gson = new Gson();

        try {
            return gson.fromJson(gson.toJson(event.getBody().get("Data Section")), DataSection.class);
        } catch (JsonSyntaxException ex) {
            LOG.log(Level.SEVERE, "getDataSectionFromEvent() - Error al intentar obtener un 'DataSection' de un evento", ex.getMessage());
            return null;
        }
    }

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
}
