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

package io.elasticjob.lite.util.json;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.elasticjob.lite.api.JobType;
import io.elasticjob.lite.config.JobCoreConfiguration;
import io.elasticjob.lite.config.JobRootConfiguration;
import io.elasticjob.lite.config.JobTypeConfiguration;
import io.elasticjob.lite.config.dataflow.DataflowJobConfiguration;
import io.elasticjob.lite.config.job.CalendarIntervalJobSchedule;
import io.elasticjob.lite.config.job.CronJobSchedule;
import io.elasticjob.lite.config.job.DailyTimeIntervalJobSchedule;
import io.elasticjob.lite.config.job.JobSchedule;
import io.elasticjob.lite.config.script.ScriptJobConfiguration;
import io.elasticjob.lite.config.simple.SimpleJobConfiguration;
import io.elasticjob.lite.executor.handler.JobProperties;
import org.quartz.DateBuilder;
import org.quartz.TimeOfDay;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * 作业配置的Json转换适配器.
 *
 * @param <T> 作业配置对象泛型
 * @author zhangliang
 * @author caohao
 */
public abstract class AbstractJobConfigurationGsonTypeAdapter<T extends JobRootConfiguration> extends TypeAdapter<T> {

    @Override
    public T read(final JsonReader in) throws IOException {
        String jobName = "";
        JobSchedule jobSchedule = null;
        String cron = "";
        int shardingTotalCount = 0;
        String shardingItemParameters = "";
        String jobParameter = "";
        boolean failover = false;
        boolean misfire = failover;
        String description = "";
        JobProperties jobProperties = new JobProperties();
        JobType jobType = null;
        String jobClass = "";
        boolean streamingProcess = false;
        String scriptCommandLine = "";
        Map<String, Object> customizedValueMap = new HashMap<>(32, 1);
        in.beginObject();
        while (in.hasNext()) {
            String jsonName = in.nextName();
            switch (jsonName) {
                case "jobName":
                    jobName = in.nextString();
                    break;
                case "cron":
                    cron = in.nextString();
                    break;
                case "schedule":
                    jobSchedule = readJobSchedule(in);
                    break;
                case "shardingTotalCount":
                    shardingTotalCount = in.nextInt();
                    break;
                case "shardingItemParameters":
                    shardingItemParameters = in.nextString();
                    break;
                case "jobParameter":
                    jobParameter = in.nextString();
                    break;
                case "failover":
                    failover = in.nextBoolean();
                    break;
                case "misfire":
                    misfire = in.nextBoolean();
                    break;
                case "description":
                    description = in.nextString();
                    break;
                case "jobProperties":
                    jobProperties = getJobProperties(in);
                    break;
                case "jobType":
                    jobType = JobType.valueOf(in.nextString());
                    break;
                case "jobClass":
                    jobClass = in.nextString();
                    break;
                case "streamingProcess":
                    streamingProcess = in.nextBoolean();
                    break;
                case "scriptCommandLine":
                    scriptCommandLine = in.nextString();
                    break;
                default:
                    addToCustomizedValueMap(jsonName, in, customizedValueMap);
                    break;
            }
        }
        in.endObject();

        if (jobSchedule == null && !Strings.isNullOrEmpty(cron)) {
            jobSchedule = new CronJobSchedule(cron);
        }

        JobCoreConfiguration coreConfig = getJobCoreConfiguration(jobName, jobSchedule, shardingTotalCount, shardingItemParameters,
                jobParameter, failover, misfire, description, jobProperties);
        JobTypeConfiguration typeConfig = getJobTypeConfiguration(coreConfig, jobType, jobClass, streamingProcess, scriptCommandLine);
        return getJobRootConfiguration(typeConfig, customizedValueMap);
    }

    private JobProperties getJobProperties(final JsonReader in) throws IOException {
        JobProperties result = new JobProperties();
        in.beginObject();
        while (in.hasNext()) {
            switch (in.nextName()) {
                case "job_exception_handler":
                    result.put(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), in.nextString());
                    break;
                case "executor_service_handler":
                    result.put(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), in.nextString());
                    break;
                default:
                    break;
            }
        }
        in.endObject();
        return result;
    }

    protected abstract void addToCustomizedValueMap(final String jsonName, final JsonReader in, final Map<String, Object> customizedValueMap) throws IOException;

    private JobCoreConfiguration getJobCoreConfiguration(final String jobName, final JobSchedule jobSchedule, final int shardingTotalCount,
            final String shardingItemParameters, final String jobParameter, final boolean failover,
            final boolean misfire, final String description,
            final JobProperties jobProperties) {
        return JobCoreConfiguration.newJobScheduleBuilder(jobName, jobSchedule, shardingTotalCount)
                .shardingItemParameters(shardingItemParameters).jobParameter(jobParameter).failover(failover).misfire(misfire).description(description)
                .jobProperties(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), jobProperties.get(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER))
                .jobProperties(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), jobProperties.get(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER))
                .build();
    }

    private JobTypeConfiguration getJobTypeConfiguration(
            final JobCoreConfiguration coreConfig, final JobType jobType, final String jobClass, final boolean streamingProcess, final String scriptCommandLine) {
        Preconditions.checkNotNull(jobType, "jobType cannot be null.");
        switch (jobType) {
            case SIMPLE:
                Preconditions.checkArgument(!Strings.isNullOrEmpty(jobClass), "jobClass cannot be empty.");
                return new SimpleJobConfiguration(coreConfig, jobClass);
            case DATAFLOW:
                Preconditions.checkArgument(!Strings.isNullOrEmpty(jobClass), "jobClass cannot be empty.");
                return new DataflowJobConfiguration(coreConfig, jobClass, streamingProcess);
            case SCRIPT:
                return new ScriptJobConfiguration(coreConfig, scriptCommandLine);
            default:
                throw new UnsupportedOperationException(String.valueOf(jobType));
        }
    }

    protected abstract T getJobRootConfiguration(final JobTypeConfiguration typeConfig, final Map<String, Object> customizedValueMap);

    @Override
    public void write(final JsonWriter out, final T value) throws IOException {
        JobTypeConfiguration typeConfig = value.getTypeConfig();
        JobCoreConfiguration coreConfig = typeConfig.getCoreConfig();

        out.beginObject();
        out.name("jobName").value(coreConfig.getJobName());
        out.name("jobClass").value(typeConfig.getJobClass());
        out.name("jobType").value(typeConfig.getJobType().name());

        writeJobSchedule(out, coreConfig);

        out.name("shardingTotalCount").value(coreConfig.getShardingTotalCount());
        out.name("shardingItemParameters").value(coreConfig.getShardingItemParameters());
        out.name("jobParameter").value(coreConfig.getJobParameter());
        out.name("failover").value(coreConfig.isFailover());
        out.name("misfire").value(coreConfig.isMisfire());
        out.name("description").value(coreConfig.getDescription());
        out.name("jobProperties").jsonValue(coreConfig.getJobProperties().json());
        if (typeConfig.getJobType() == JobType.DATAFLOW) {
            DataflowJobConfiguration dataflowJobConfig = (DataflowJobConfiguration) typeConfig;
            out.name("streamingProcess").value(dataflowJobConfig.isStreamingProcess());
        } else if (typeConfig.getJobType() == JobType.SCRIPT) {
            ScriptJobConfiguration scriptJobConfig = (ScriptJobConfiguration) typeConfig;
            out.name("scriptCommandLine").value(scriptJobConfig.getScriptCommandLine());
        }
        writeCustomized(out, value);
        out.endObject();
    }

    private void writeJobSchedule(JsonWriter out, JobCoreConfiguration coreConfig) throws IOException {
        JobSchedule jobSchedule = coreConfig.getSchedule();
        JobSchedule.ScheduleType scheduleType = jobSchedule.getType();
        JsonWriter scheduleOut;
        switch (scheduleType) {
            case CRON:
                out.name("cron").value(((CronJobSchedule) jobSchedule).getCron());
                break;
            case DAILY_TIME:
                scheduleOut = out.name("schedule").beginObject().name("type").value(scheduleType.name());

                DailyTimeIntervalJobSchedule dailyTimeIntervalJobSchedule = (DailyTimeIntervalJobSchedule) jobSchedule;
                scheduleOut.name("interval").value(dailyTimeIntervalJobSchedule.getInterval())
                        .name("intervalUnit").value(dailyTimeIntervalJobSchedule.getIntervalUnit().name());
                if (dailyTimeIntervalJobSchedule.getDaysOfWeek() != null && !dailyTimeIntervalJobSchedule.getDaysOfWeek().isEmpty()) {
                    scheduleOut.name("daysOfWeek").value(dailyTimeIntervalJobSchedule.getDaysOfWeek().stream().map(String::valueOf).collect(Collectors.joining()));
                }
                writeTimeOfDay(scheduleOut, "startTimeOfDay", dailyTimeIntervalJobSchedule.getStartTimeOfDay());
                writeTimeOfDay(scheduleOut, "endTimeOfDay", dailyTimeIntervalJobSchedule.getEndTimeOfDay());

                scheduleOut.endObject();
                break;
            case CALENDAR:
                scheduleOut = out.name("schedule").beginObject().name("type").value(scheduleType.name());

                CalendarIntervalJobSchedule calendarIntervalJobSchedule = (CalendarIntervalJobSchedule) jobSchedule;
                scheduleOut.name("interval").value(calendarIntervalJobSchedule.getInterval())
                        .name("intervalUnit").value(calendarIntervalJobSchedule.getIntervalUnit().name());

                if (calendarIntervalJobSchedule.getTimeZone() != null) {
                    scheduleOut.name("timeZone").value(calendarIntervalJobSchedule.getTimeZone().getID());
                }

                scheduleOut.endObject();
                break;
            default:
                throw new IllegalStateException("un-handled switch case ScheduleType." + scheduleType);
        }

    }

    private void writeTimeOfDay(JsonWriter scheduleOut, String name, TimeOfDay timeOfDay) throws IOException {
        if (timeOfDay != null) {
            scheduleOut.name(name).beginObject()
                    .name("hour").value(timeOfDay.getHour())
                    .name("minute").value(timeOfDay.getMinute())
                    .name("seconds").value(timeOfDay.getSecond())
                    .endObject();
        }
    }

    private JobSchedule readJobSchedule(JsonReader in) throws IOException {
        JobSchedule.ScheduleType type = null;
        String cron = null;
        Integer interval = null;
        DateBuilder.IntervalUnit intervalUnit = null;
        String daysOfWeek = null;
        String timeZoneId = null;
        TimeOfDay startTimeOfDay = null;
        TimeOfDay endTimeOfDay = null;

        in.beginObject();
        while (in.hasNext()) {
            String name = in.nextName();
            switch (name) {
                case "type":
                    type = JobSchedule.ScheduleType.valueOf(in.nextString());
                    break;
                case "cron":
                    cron = in.nextString();
                    break;
                case "interval":
                    interval = in.nextInt();
                    break;
                case "intervalUnit":
                    intervalUnit = DateBuilder.IntervalUnit.valueOf(in.nextString());
                    break;
                case "daysOfWeek":
                    daysOfWeek = in.nextString();
                    break;
                case "startTimeOfDay":
                    startTimeOfDay = readTimeOfDay(in);
                    break;
                case "endTimeOfDay":
                    endTimeOfDay = readTimeOfDay(in);
                    break;
                case "timeZone":
                    timeZoneId = in.nextString();
                    break;
                default:
                    throw new IllegalStateException("un-handled json property: " + name);
            }
        }
        in.endObject();

        if (type == null) {
            throw new IllegalStateException("解析JSON失败，未发现schedule.type属性");
        }

        switch (type) {
            case CRON:
                return new CronJobSchedule(cron);
            case DAILY_TIME:
                DailyTimeIntervalJobSchedule dailyTimeIntervalJobSchedule = new DailyTimeIntervalJobSchedule(interval, intervalUnit);

                if (!Strings.isNullOrEmpty(daysOfWeek)) {
                    Set<Integer> daysOfWeekSet = Arrays.stream(daysOfWeek.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
                    dailyTimeIntervalJobSchedule.setDaysOfWeek(daysOfWeekSet);
                }
                dailyTimeIntervalJobSchedule.setStartTimeOfDay(startTimeOfDay);
                dailyTimeIntervalJobSchedule.setEndTimeOfDay(endTimeOfDay);

                return dailyTimeIntervalJobSchedule;
            case CALENDAR:
                CalendarIntervalJobSchedule calendarIntervalJobSchedule = new CalendarIntervalJobSchedule(interval, intervalUnit);
                if (!Strings.isNullOrEmpty(timeZoneId)) {
                    calendarIntervalJobSchedule.setTimeZone(TimeZone.getTimeZone(timeZoneId));
                }

                return calendarIntervalJobSchedule;
            default:
                throw new IllegalStateException("un-handled switch case ScheduleType." + type);
        }
    }

    private TimeOfDay readTimeOfDay(JsonReader in) throws IOException {
        int hour = 0;
        int minute = 0;
        int second = 0;

        in.beginObject();
        while (in.hasNext()) {
            String name = in.nextName();
            switch (name) {
                case "hour":
                    hour = in.nextInt();
                    break;
                case "minute":
                    minute = in.nextInt();
                    break;
                case "second":
                    second = in.nextInt();
                    break;
                default:
                    // do nothing
            }
        }
        in.endObject();

        return new TimeOfDay(hour, minute, second);
    }

    protected abstract void writeCustomized(final JsonWriter out, final T value) throws IOException;
}
