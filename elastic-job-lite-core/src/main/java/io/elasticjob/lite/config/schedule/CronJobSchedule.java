package io.elasticjob.lite.config.schedule;

import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * @author hotleave
 * @date 2019-02-18
 */
@Data
@RequiredArgsConstructor
public class CronJobSchedule implements JobSchedule {
    private final String cron;

    @Override
    public ScheduleType getType() {
        return ScheduleType.CRON;
    }
}
