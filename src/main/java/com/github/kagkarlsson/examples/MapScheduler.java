package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.PollingStrategyConfig;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerName;
import com.github.kagkarlsson.scheduler.TaskRepository;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.Waiter;
import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.task.OnStartup;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class MapScheduler extends Scheduler {

    public MapScheduler(Clock clock, TaskRepository schedulerTaskRepository, TaskRepository clientTaskRepository, TaskResolver taskResolver, int threadpoolSize, ExecutorService executorService, SchedulerName schedulerName, Waiter executeDueWaiter, Duration heartbeatInterval, int numberOfMissedHeartbeatsBeforeDead, List<SchedulerListener> schedulerListeners, List<ExecutionInterceptor> executionInterceptors, PollingStrategyConfig pollingStrategyConfig, Duration deleteUnresolvedAfter, Duration shutdownMaxWait, LogLevel logLevel, boolean logStackTrace, List<OnStartup> onStartup, ExecutorService dueExecutor, ScheduledExecutorService housekeeperExecutor) {
        super(clock, schedulerTaskRepository, clientTaskRepository, taskResolver, threadpoolSize, executorService, schedulerName, executeDueWaiter, heartbeatInterval, numberOfMissedHeartbeatsBeforeDead, schedulerListeners, executionInterceptors, pollingStrategyConfig, deleteUnresolvedAfter, shutdownMaxWait, logLevel, logStackTrace, onStartup, dueExecutor, housekeeperExecutor);
    }

}
