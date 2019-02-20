package io.elasticjob.lite.config.schedule;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.quartz.DateBuilder.IntervalUnit;

/**
 * @author hotleave
 * @date 2019-02-18
 */
@Data
@RequiredArgsConstructor
public class CalendarIntervalJobSchedule implements JobSchedule {
    private final Integer interval;
    private final IntervalUnit intervalUnit;

    private String timeZone;

    @Override
    public ScheduleType getType() {
        return ScheduleType.CALENDAR;
    }
}
