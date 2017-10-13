package com.onur.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by Onur_Dincol on 10/6/2017.
 */
public class TimeOperations {
    private static final String DATE_FORMAT = "YYYY-MM-DD hh:mm:ss";

    public String changePSTtoUTC(String pstDate)
    {
        LocalDateTime pstDateTime = LocalDateTime.parse(pstDate, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        ZoneId pstZoneId = ZoneId.of("America/Los_Angeles");
        ZonedDateTime pstZonedDateTime = pstDateTime.atZone(pstZoneId);
        ZoneId utcZoneId = ZoneId.of("UTC");
        ZonedDateTime utcDateTime = pstZonedDateTime.withZoneSameInstant(utcZoneId);
        DateTimeFormatter format = DateTimeFormatter.ofPattern(DATE_FORMAT);
        return utcDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
