/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.elasticjob.lite.internal.schedule;

import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.job.CalendarIntervalJobSchedule;
import io.elasticjob.lite.config.job.CronJobSchedule;
import io.elasticjob.lite.config.job.DailyTimeIntervalJobSchedule;
import io.elasticjob.lite.config.job.JobSchedule;
import io.elasticjob.lite.exception.JobSystemException;
import lombok.RequiredArgsConstructor;
import org.quartz.CalendarIntervalScheduleBuilder;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DailyTimeIntervalScheduleBuilder;
import org.quartz.JobDetail;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import java.util.Set;
import java.util.TimeZone;

/**
 * 作业调度控制器.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class JobScheduleController {

    private final Scheduler scheduler;

    private final JobDetail jobDetail;

    private final String triggerIdentity;

    /**
     * 调度作业.
     *
     * @param configuration 作业配置
     */
    public void scheduleJob(final JobCoreConfiguration configuration) {
        try {
            if (!scheduler.checkExists(jobDetail.getKey())) {
                scheduler.scheduleJob(jobDetail, createTrigger(configuration.getSchedule()));
            }
            scheduler.start();
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 重新调度作业.
     *
     * @param configuration 作业配置
     */
    public void rescheduleJob(final JobCoreConfiguration configuration) {
        rescheduleJob(configuration.getSchedule());
    }

    public void rescheduleJob(final String cron) {
        rescheduleJob(new CronJobSchedule(cron));
    }

    private synchronized void rescheduleJob(JobSchedule jobSchedule) {
        try {
            Trigger trigger = scheduler.getTrigger(TriggerKey.triggerKey(triggerIdentity));
            if (!scheduler.isShutdown() && null != trigger) {
                scheduler.rescheduleJob(TriggerKey.triggerKey(triggerIdentity), createTrigger(jobSchedule));
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    private Trigger createTrigger(final JobSchedule jobSchedule) {
        ScheduleBuilder<? extends Trigger> scheduleBuilder;

        switch (jobSchedule.getType()) {
            case CRON:
                scheduleBuilder = createCronScheduleBuilder((CronJobSchedule) jobSchedule);
                break;
            case DAILY_TIME:
                scheduleBuilder = createDailyTimeIntervalScheduleBuilder((DailyTimeIntervalJobSchedule) jobSchedule);
                break;
            case CALENDAR:
                scheduleBuilder = createCalendarIntervalScheduleBuilder((CalendarIntervalJobSchedule) jobSchedule);
                break;
            default:
                throw new IllegalStateException("un-handled switch case: ScheduleType." + jobSchedule.getType());
        }

        return TriggerBuilder.newTrigger().withIdentity(triggerIdentity).withSchedule(scheduleBuilder).build();
    }

    private ScheduleBuilder<CronTrigger> createCronScheduleBuilder(CronJobSchedule schedule) {
        return CronScheduleBuilder.cronSchedule(schedule.getCron()).withMisfireHandlingInstructionDoNothing();
    }

    private ScheduleBuilder<? extends Trigger> createDailyTimeIntervalScheduleBuilder(DailyTimeIntervalJobSchedule schedule) {
        DailyTimeIntervalScheduleBuilder scheduleBuilder = DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
                .withInterval(schedule.getInterval(), schedule.getIntervalUnit())
                .withMisfireHandlingInstructionDoNothing();

        Set<Integer> daysOfWeek = schedule.getDaysOfWeek();
        if (daysOfWeek != null && !daysOfWeek.isEmpty()) {
            scheduleBuilder.onDaysOfTheWeek(daysOfWeek);
        }

        TimeOfDay startTime = schedule.getStartTimeOfDay();
        if (startTime != null) {
            scheduleBuilder.startingDailyAt(startTime);
        }

        TimeOfDay endTime = schedule.getEndTimeOfDay();
        if (endTime != null) {
            scheduleBuilder.endingDailyAt(endTime);
        }

        return scheduleBuilder;
    }

    private ScheduleBuilder<? extends Trigger> createCalendarIntervalScheduleBuilder(CalendarIntervalJobSchedule schedule) {
        CalendarIntervalScheduleBuilder scheduleBuilder = CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withInterval(schedule.getInterval(), schedule.getIntervalUnit())
                .withMisfireHandlingInstructionDoNothing();

        TimeZone timeZone = schedule.getTimeZone();
        if (timeZone != null) {
            scheduleBuilder.inTimeZone(timeZone);
        }

        return scheduleBuilder;
    }

    /**
     * 判断作业是否暂停.
     *
     * @return 作业是否暂停
     */
    public synchronized boolean isPaused() {
        try {
            return !scheduler.isShutdown() && Trigger.TriggerState.PAUSED == scheduler.getTriggerState(new TriggerKey(triggerIdentity));
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 暂停作业.
     */
    public synchronized void pauseJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.pauseAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 恢复作业.
     */
    public synchronized void resumeJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.resumeAll();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 立刻启动作业.
     */
    public synchronized void triggerJob() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.triggerJob(jobDetail.getKey());
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }

    /**
     * 关闭调度器.
     */
    public synchronized void shutdown() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.shutdown();
            }
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
    }
}
