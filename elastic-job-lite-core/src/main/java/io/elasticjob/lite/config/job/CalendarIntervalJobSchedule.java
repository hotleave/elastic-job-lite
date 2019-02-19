package io.elasticjob.lite.config.job;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.quartz.DateBuilder.IntervalUnit;

import java.util.TimeZone;

/**
 * @author hotleave
 * @date 2019-02-18
 */
@Data
@RequiredArgsConstructor
public class CalendarIntervalJobSchedule implements JobSchedule {
    private final Integer interval;
    private final IntervalUnit intervalUnit;

    private TimeZone timeZone;

    @Override
    public ScheduleType getType() {
        return ScheduleType.CALENDAR;
    }
}
