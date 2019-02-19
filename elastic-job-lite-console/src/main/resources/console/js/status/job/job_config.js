$(function() {
    tooltipLocale();
    validate();
    bindSubmitJobSettingsForm();
    bindResetForm();
    bindScheduleTypeChange();
});

function tooltipLocale(){
    for (var i = 0; i < $("[data-toggle='tooltip']").length; i++) {
        var object = $("[data-toggle='tooltip']")[i];
        $(object).attr('title',$.i18n.prop("placeholder-" + object.getAttribute("id"))).tooltip('fixTitle');
    }
}

function getJobParams() {
    var jobName = $("#job-overviews-name").text();
    var jobParams;
    $.ajax({
        url: "/api/jobs/config/" + jobName,
        async: false,
        success: function(data) {
            jobParams = data;
        }
    });
    return jobParams;
}

function getTimeOfDay(timeOfDay) {
    if (!timeOfDay) return;

    var values = timeOfDay.split(":");
    return { hour: values[0], minute: values[1], second: values[2] }
}

function getScheduleParams() {
    var type = $("#job-schedule-type").val();
    var cron = $("#job-schedule-cron").val();
    var interval = $("#job-schedule-interval").val();
    var intervalUnit = $("#job-schedule-interval-unit").val();
    var timeZone = $("#job-schedule-timezone").val();
    var startTimeOfDay = $("#job-schedule-start-time-of-day").val();
    var endTimeOfDay = $("#job-schedule-end-time-of-day").val();
    var daysOfWeek = $('input[name=daysOfWeek]:checked').map(function (index, value) {
        return $(value).val()
    }).toArray().join(",");
    var params = {};

    switch (type) {
        case "CRON":
            params = {cron: cron};
            break;
        case "DAILY_TIME":
            params = {
                interval: interval,
                intervalUnit: intervalUnit,
                startTimeOfDay: getTimeOfDay(startTimeOfDay),
                endTimeOfDay: getTimeOfDay(endTimeOfDay),
                daysOfWeek: daysOfWeek
            };
            break;
        case "CALENDAR":
            params = {interval: interval, intervalUnit: intervalUnit, timeZone: timeZone};
            break;
    }

    params.type = type;
    return params;
}

function bindSubmitJobSettingsForm() {
    $("#update-job-info-btn").on("click", function(){
        var bootstrapValidator = $("#job-config-form").data("bootstrapValidator");
        bootstrapValidator.validate();
        if (bootstrapValidator.isValid()) {
            var jobName = $("#job-name").val();
            var jobType = $("#job-type").val();
            var jobClass = $("#job-class").val();
            var shardingTotalCount = $("#sharding-total-count").val();
            var jobParameter = $("#job-parameter").val();
            var schedule = getScheduleParams();
            var streamingProcess = $("#streaming-process").prop("checked");
            var maxTimeDiffSeconds = $("#max-time-diff-seconds").val();
            var monitorPort = $("#monitor-port").val();
            var monitorExecution = $("#monitor-execution").prop("checked");
            var failover = $("#failover").prop("checked");
            var misfire = $("#misfire").prop("checked");
            var driver = $("#driver").val();
            var url = $("#url").val();
            var username = $("#username").val();
            var password = $("#password").val();
            var logLevel = $("#logLevel").val();
            var shardingItemParameters = $("#sharding-item-parameters").val();
            var jobShardingStrategyClass = $("#job-sharding-strategy-class").val();
            var scriptCommandLine = $("#script-command-line").val();
            var executorServiceHandler = $("#executor-service-handler").val();
            var jobExceptionHandler = $("#job-exception-handler").val();
            var description = $("#description").val();
            var reconcileIntervalMinutes = $("#reconcile-interval-minutes").val();
            var postJson = {jobName: jobName, jobType : jobType, jobClass : jobClass, shardingTotalCount: shardingTotalCount, jobParameter: jobParameter, schedule: schedule, streamingProcess: streamingProcess, maxTimeDiffSeconds: maxTimeDiffSeconds, monitorPort: monitorPort, monitorExecution: monitorExecution, failover: failover, misfire: misfire, shardingItemParameters: shardingItemParameters, jobShardingStrategyClass: jobShardingStrategyClass, jobProperties: {"executor_service_handler": executorServiceHandler, "job_exception_handler": jobExceptionHandler}, description: description, scriptCommandLine: scriptCommandLine, reconcileIntervalMinutes:reconcileIntervalMinutes};
            var jobParams = getJobParams();
            if (jobParams.monitorExecution !== monitorExecution || jobParams.failover !== failover || jobParams.misfire !== misfire) {
                showUpdateConfirmModal();
                $(document).off("click", "#confirm-btn");
                $(document).on("click", "#confirm-btn", function() {
                    $("#confirm-dialog").modal("hide");
                    submitAjax(postJson);
                });
            } else {
                submitAjax(postJson);
            }
        }
    });
}

function submitAjax(postJson) {
    $.ajax({
        url: "/api/jobs/config",
        type: "PUT",
        data: JSON.stringify(postJson),
        contentType: "application/json",
        dataType: "json",
        success: function() {
            $("#data-update-job").modal("hide");
            $("#jobs-status-overview-tbl").bootstrapTable("refresh");
            showSuccessDialog();
        }
    });
}

function validate() {
    $("#job-config-form").bootstrapValidator({
        message: "This value is not valid",
        feedbackIcons: {
            valid: "glyphicon glyphicon-ok",
            invalid: "glyphicon glyphicon-remove",
            validating: "glyphicon glyphicon-refresh"
        },
        fields: {
            shardingTotalCount: {
                validators: {
                    notEmpty: {
                        message: $.i18n.prop("job-sharding-count-not-null")
                    },
                    regexp: {
                        regexp: /^(-?\d+)?$/,
                        message: $.i18n.prop("job-sharding-count-should-be-integer")
                    }
                }
            },
            cron: {
                validators: {
                    stringLength: {
                        max: 40,
                        message: $.i18n.prop("job-cron-length-limit")
                    },
                    notEmpty: {
                        message: $.i18n.prop("job-cron-not-null")
                    }
                }
            },
            monitorPort: {
                validators: {
                    regexp: {
                        regexp: /^(-?\d+)?$/,
                        message: $.i18n.prop("job-monitor-port-should-be-integer")
                    },
                    notEmpty: {
                        message: $.i18n.prop("job-monitor-port-not-null")
                    },
                    callback: {
                        message: $.i18n.prop("job-monitor-port-range-limit"),
                        callback: function(value, validator) {
                            var monitorPort = parseInt(validator.getFieldElements("monitorPort").val(), 10);
                            if (monitorPort <= 65535) {
                                validator.updateStatus("monitorPort", "VALID");
                                return true;
                            }
                            return false;
                        }
                    }
                }
            }
        }
    });
    $("#job-config-form").submit(function(event) {
        event.preventDefault();
    });
}

function bindResetForm() {
    $("#reset").click(function() {
        $("#job-config-form").data("bootstrapValidator").resetForm();
    });
}

function bindScheduleTypeChange() {
    var intervalUnitOptions = [
        { label: "毫秒", value: "MILLISECOND" },
        { label: "秒", value: "SECOND" },
        { label: "分钟", value: "MINUTE" },
        { label: "小时", value: "HOUR" },
        { label: "天", value: "DAY" },
        { label: "周", value: "WEEK" },
        { label: "月", value: "MONTH" },
        { label: "年", value: "YEAR" }
    ];
    var toggleIntervalUnitOptions = function(options) {
        var $intervalUnit = $("#job-schedule-interval-unit");
        $intervalUnit.empty();
        $.each(options, function (k, v) {
            $intervalUnit.append($("<option/>").attr("value", v.value).text(v.label));
        });
    };

    $("#job-schedule-type").change(function () {
        var val = $(this).val();
        switch (val) {
            case "CRON":
                $("#interval-config-row, #daily-config-row, #daily-config-weekday").hide();
                $("#job-cron-config").show();
                break;
            case "DAILY_TIME":
                toggleIntervalUnitOptions(intervalUnitOptions.slice(1, 4));
                $("#interval-config-row, #daily-config-row, #daily-config-weekday").show();
                $("#job-cron-config, #job-timezone-config").hide();
                break;
            case "CALENDAR":
                toggleIntervalUnitOptions(intervalUnitOptions);
                $("#interval-config-row, #job-timezone-config").show();
                $("#job-cron-config, #daily-config-row, #daily-config-weekday").hide();
                break;
        }
    }).change();
}
