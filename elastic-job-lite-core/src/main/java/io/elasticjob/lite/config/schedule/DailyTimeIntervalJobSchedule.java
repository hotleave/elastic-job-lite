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
public class DailyTimeIntervalJobSchedule implements JobSchedule {
    /**
     * 执行间隔
     */
    private final Integer interval;
    private final IntervalUnit intervalUnit;
    /**
     * 星期
     *
     * 1-7 => 周日到周六
     */
    private String daysOfWeek;
    private String startTimeOfDay;
    private String endTimeOfDay;

    @Override
    public ScheduleType getType() {
        return ScheduleType.DAILY_TIME;
    }
}
