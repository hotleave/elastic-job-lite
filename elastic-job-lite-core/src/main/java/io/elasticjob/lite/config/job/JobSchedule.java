package io.elasticjob.lite.config.job;

import org.quartz.CalendarIntervalTrigger;
import org.quartz.CronTrigger;
import org.quartz.DailyTimeIntervalTrigger;

/**
 * @author hotleave
 * @date 2019-02-18
 */
public interface JobSchedule {
    /**
     * 作业类型
     *
     * @return 类型
     */
    ScheduleType getType();

    enum ScheduleType {
        /**
         * CRON
         *
         * @see CronTrigger
         */
        CRON,
        /**
         * 每日
         *
         * @see DailyTimeIntervalTrigger
         */
        DAILY_TIME,
        /**
         * 灵活周期
         *
         * @see CalendarIntervalTrigger
         */
        CALENDAR
    }
}
