package com.onur.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by Onur_Dincol on 10/6/2017.
 */
public class TimeOperations {
    public String changePSTtoUTC(String pstDate)
    {
        DateTimeZone californiaTimeZone = DateTimeZone.forID("America/Los_Angeles");
        DateTimeFormatter dateStringFormat = DateTimeFormat.forPattern("YYYY-MM-DD HH:mm:ss").withZone(californiaTimeZone);
        DateTime californiaDateTime = dateStringFormat.parseDateTime(pstDate);
        DateTime utcDateTime = californiaDateTime.toDateTime(DateTimeZone.UTC);
        DateTimeFormatter dtf = DateTimeFormat.forPattern("YYYY-MM-DD HH:mm:ss");
        return dtf.print(utcDateTime) + " UTC";
    }
}
