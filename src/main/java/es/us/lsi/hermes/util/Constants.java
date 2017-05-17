package es.us.lsi.hermes.util;

import java.text.SimpleDateFormat;
import java.util.logging.Logger;

public class Constants {

    private static final Logger LOG = Logger.getLogger(Constants.class.getName());
    public static final SimpleDateFormat dfISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
}
