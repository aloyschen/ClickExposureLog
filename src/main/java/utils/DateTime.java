package utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTime {

    public int getDateMinute(){
        Calendar now = Calendar.getInstance();
        return now.get(Calendar.MINUTE);
    }

    public int getDateHour(){
        Calendar now = Calendar.getInstance();
        return now.get(Calendar.HOUR_OF_DAY);
    }

    public String getNowDate(){
        Calendar now = Calendar.getInstance();
        now.get(Calendar.HOUR_OF_DAY - 1);
        Date date = now.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String nowDate = sdf.format(date);
        return nowDate;
    }

}
