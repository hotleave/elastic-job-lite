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

package io.elasticjob.lite.lifecycle.domain;

import io.elasticjob.lite.executor.handler.JobProperties.JobPropertiesEnum;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 作业设置对象.
 *
 * @author zhangliang
 */
@Getter
@Setter
public final class JobSettings implements Serializable {

    private static final long serialVersionUID = -6532210090618686688L;

    private String jobName;

    private String jobType;

    private String jobClass;

    private String cron;

    private JobScheduleSettings schedule;

    private int shardingTotalCount;

    private String shardingItemParameters;

    private String jobParameter;

    private boolean monitorExecution;

    private boolean streamingProcess;

    private int maxTimeDiffSeconds;

    private int monitorPort = -1;

    private boolean failover;

    private boolean misfire;

    private String jobShardingStrategyClass;

    private String description;

    private Map<String, String> jobProperties = new LinkedHashMap<>(JobPropertiesEnum.values().length, 1);

    private String scriptCommandLine;

    private int reconcileIntervalMinutes;

    @Data
    public static class JobScheduleSettings {
        private String type;
        private String cron;
        private Integer interval;
        private String intervalUnit;
        private String daysOfWeek;
        private TimeOfDay startTimeOfDay;
        private TimeOfDay endTimeOfDay;
        private String timeZone;
    }

    @Data
    public static class TimeOfDay {
        private Integer hour;
        private Integer minute;
        private Integer second;

        public static TimeOfDay valueOf(org.quartz.TimeOfDay source) {
            if (source == null) {
                return null;
            }

            TimeOfDay result = new TimeOfDay();
            result.hour = source.getHour();
            result.minute = source.getMinute();
            result.second = source.getSecond();

            return result;
        }
    }
}
