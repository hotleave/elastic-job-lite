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

import com.google.common.base.Strings;
import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.schedule.CalendarIntervalJobSchedule;
import io.elasticjob.lite.config.schedule.CronJobSchedule;
import io.elasticjob.lite.config.schedule.DailyTimeIntervalJobSchedule;
import io.elasticjob.lite.config.schedule.JobSchedule;
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

import java.util.Arrays;
import java.util.TimeZone;
import java.util.stream.Collectors;

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

        String daysOfWeek = schedule.getDaysOfWeek();
        if (!Strings.isNullOrEmpty(daysOfWeek)) {
            scheduleBuilder.onDaysOfTheWeek(Arrays.stream(daysOfWeek.split(",")).map(Integer::valueOf).collect(Collectors.toSet()));
        }

        TimeOfDay startTime = getTimeOfDay(schedule.getStartTimeOfDay());
        if (startTime != null) {
            scheduleBuilder.startingDailyAt(startTime);
        }

        TimeOfDay endTime = getTimeOfDay(schedule.getEndTimeOfDay());
        if (endTime != null) {
            scheduleBuilder.endingDailyAt(endTime);
        }

        return scheduleBuilder;
    }

    private ScheduleBuilder<? extends Trigger> createCalendarIntervalScheduleBuilder(CalendarIntervalJobSchedule schedule) {
        CalendarIntervalScheduleBuilder scheduleBuilder = CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withInterval(schedule.getInterval(), schedule.getIntervalUnit())
                .withMisfireHandlingInstructionDoNothing();

        if (!Strings.isNullOrEmpty(schedule.getTimeZone())) {
            TimeZone timeZone = TimeZone.getTimeZone(schedule.getTimeZone());
            if (timeZone != null) {
                scheduleBuilder.inTimeZone(timeZone);
            }
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

    private TimeOfDay getTimeOfDay(String value) {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        String[] values = value.split(":");
        if (values.length != 3) {
            throw new IllegalArgumentException("startTimeOfDay or endTimeOfDay must match the format of HH:mm:ss");
        }
        int[] params = Arrays.stream(values).mapToInt(Integer::parseInt).toArray();

        return new TimeOfDay(params[0], params[1], params[2]);
    }
}
