package io.elasticjob.lite.config.job;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.TimeOfDay;

import java.util.Set;

/**
 * @author hotleave
 * @date 2019-02-18
 */
@Data
@RequiredArgsConstructor
public class DailyTimeIntervalJobSchedule implements JobSchedule {
    /**
     * 执行间隔
     */
    private final Integer interval;
    private final IntervalUnit intervalUnit;

    private Set<Integer> daysOfWeek;
    private TimeOfDay startTimeOfDay;
    private TimeOfDay endTimeOfDay;

    @Override
    public ScheduleType getType() {
        return ScheduleType.DAILY_TIME;
    }
}
