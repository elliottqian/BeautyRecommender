package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hzqianwei on 2016/8/5.
 *
 */
public class DataUtils {
    public static String getTodayString() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(now);
    }
}
