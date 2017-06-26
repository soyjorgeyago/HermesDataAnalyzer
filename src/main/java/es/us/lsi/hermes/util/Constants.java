package es.us.lsi.hermes.util;

import java.text.SimpleDateFormat;

public class Constants {

    public static final SimpleDateFormat dfISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final double RADIUS = 100;
    public static final double SURROUNDING_DIAMETER = 2 * RADIUS;
}
