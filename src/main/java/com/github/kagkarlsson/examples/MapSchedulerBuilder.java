package com.github.kagkarlsson.examples;

import static com.github.kagkarlsson.scheduler.ExecutorUtils.defaultThreadFactoryWithPrefix;
import static com.github.kagkarlsson.scheduler.Scheduler.THREAD_PREFIX;
import static java.util.Optional.ofNullable;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.event.ExecutionInterceptor;
import com.github.kagkarlsson.scheduler.event.SchedulerListener;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.logging.LogLevel;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.stats.StatsRegistry;
import com.github.kagkarlsson.scheduler.stats.StatsRegistryAdapter;
import com.github.kagkarlsson.scheduler.task.OnStartup;
import com.github.kagkarlsson.scheduler.task.Task;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapSchedulerBuilder {
    public static final double UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH = 3.0;
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(10);
    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMinutes(5);
    public static final int DEFAULT_MISSED_HEARTBEATS_LIMIT = 6;
    public static final Duration DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION = Duration.ofDays(14);
    public static final Duration SHUTDOWN_MAX_WAIT = Duration.ofMinutes(30);
    public static final PollingStrategyConfig DEFAULT_POLLING_STRATEGY =
            new PollingStrategyConfig(
                    PollingStrategyConfig.Type.FETCH, 0.5, UPPER_LIMIT_FRACTION_OF_THREADS_FOR_FETCH);
    public static final LogLevel DEFAULT_FAILURE_LOG_LEVEL = LogLevel.WARN;
    public static final boolean LOG_STACK_TRACE_ON_FAILURE = true;
    private static final Logger LOG = LoggerFactory.getLogger(com.github.kagkarlsson.scheduler.SchedulerBuilder.class);
    protected final DataSource dataSource;
    protected final List<Task<?>> knownTasks = new ArrayList<>();
    protected final List<OnStartup> startTasks = new ArrayList<>();
    protected Clock clock = new SystemClock(); // if this is set, waiter-clocks must be updated
    protected SchedulerName schedulerName;
    protected int executorThreads = 10;
    protected Duration poolingInterval = DEFAULT_POLLING_INTERVAL;
    protected StatsRegistry statsRegistry = StatsRegistry.NOOP;
    protected Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    protected Serializer serializer = Serializer.DEFAULT_JAVA_SERIALIZER;
    protected String tableName = JdbcTaskRepository.DEFAULT_TABLE_NAME;
    protected boolean enableImmediateExecution = false;
    protected ExecutorService executorService;
    protected ExecutorService dueExecutor;
    protected ScheduledExecutorService housekeeperExecutor;
    protected Duration deleteUnresolvedAfter = DEFAULT_DELETION_OF_UNRESOLVED_TASKS_DURATION;
    protected JdbcCustomization jdbcCustomization = null;
    protected Duration shutdownMaxWait = SHUTDOWN_MAX_WAIT;
    protected boolean commitWhenAutocommitDisabled = false;
    protected PollingStrategyConfig pollingStrategyConfig = DEFAULT_POLLING_STRATEGY;
    protected LogLevel logLevel = DEFAULT_FAILURE_LOG_LEVEL;
    protected boolean logStackTrace = LOG_STACK_TRACE_ON_FAILURE;
    protected boolean enablePriority = false;
    protected boolean registerShutdownHook = false;
    protected int numberOfMissedHeartbeatsBeforeDead = DEFAULT_MISSED_HEARTBEATS_LIMIT;
    protected boolean alwaysPersistTimestampInUTC = false;
    protected List<SchedulerListener> schedulerListeners = new ArrayList<>();
    protected List<ExecutionInterceptor> executionInterceptors = new ArrayList<>();
    protected TaskRepository taskRepository;
    protected TaskResolver taskResolver;

    public MapSchedulerBuilder(DataSource dataSource, List<Task<?>> knownTasks) {
        this.dataSource = dataSource;
        this.knownTasks.addAll(knownTasks);
    }

    @SafeVarargs
    public final <T extends Task<?> & OnStartup> MapSchedulerBuilder startTasks(T... startTasks) {
        return startTasks(Arrays.asList(startTasks));
    }

    public <T extends Task<?> & OnStartup> MapSchedulerBuilder startTasks(List<T> startTasks) {
        knownTasks.addAll(startTasks);
        this.startTasks.addAll(startTasks);
        return this;
    }

    public MapSchedulerBuilder pollingInterval(Duration pollingInterval) {
        this.poolingInterval = pollingInterval;
        return this;
    }

    public MapSchedulerBuilder heartbeatInterval(Duration duration) {
        this.heartbeatInterval = duration;
        return this;
    }

    public MapSchedulerBuilder missedHeartbeatsLimit(int numberOfMissedHeartbeatsBeforeDead) {
        if (numberOfMissedHeartbeatsBeforeDead < 4) {
            throw new IllegalArgumentException("Heartbeat-limit must be at least 4");
        }
        this.numberOfMissedHeartbeatsBeforeDead = numberOfMissedHeartbeatsBeforeDead;
        return this;
    }

    public MapSchedulerBuilder threads(int numberOfThreads) {
        this.executorThreads = numberOfThreads;
        return this;
    }

    public MapSchedulerBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    public MapSchedulerBuilder dueExecutor(ExecutorService dueExecutor) {
        this.dueExecutor = dueExecutor;
        return this;
    }

    public MapSchedulerBuilder housekeeperExecutor(ScheduledExecutorService housekeeperExecutor) {
        this.housekeeperExecutor = housekeeperExecutor;
        return this;
    }

    /** Deprecated, use addSchedulerListener instead */
    @Deprecated
    public MapSchedulerBuilder statsRegistry(StatsRegistry statsRegistry) {
        this.statsRegistry = statsRegistry;
        return this;
    }

    public MapSchedulerBuilder addSchedulerListener(SchedulerListener schedulerListener) {
        this.schedulerListeners.add(schedulerListener);
        return this;
    }

    public MapSchedulerBuilder addExecutionInterceptor(ExecutionInterceptor interceptor) {
        this.executionInterceptors.add(interceptor);
        return this;
    }

    public MapSchedulerBuilder schedulerName(SchedulerName schedulerName) {
        this.schedulerName = schedulerName;
        return this;
    }

    public MapSchedulerBuilder serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    public MapSchedulerBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public MapSchedulerBuilder enableImmediateExecution() {
        this.enableImmediateExecution = true;
        return this;
    }

    public MapSchedulerBuilder deleteUnresolvedAfter(Duration deleteAfter) {
        this.deleteUnresolvedAfter = deleteAfter;
        return this;
    }

    public MapSchedulerBuilder jdbcCustomization(JdbcCustomization jdbcCustomization) {
        this.jdbcCustomization = jdbcCustomization;
        return this;
    }

    public MapSchedulerBuilder alwaysPersistTimestampInUTC() {
        this.alwaysPersistTimestampInUTC = true;
        return this;
    }

    public MapSchedulerBuilder shutdownMaxWait(Duration shutdownMaxWait) {
        this.shutdownMaxWait = shutdownMaxWait;
        return this;
    }

    public MapSchedulerBuilder commitWhenAutocommitDisabled(boolean commitWhenAutocommitDisabled) {
        this.commitWhenAutocommitDisabled = commitWhenAutocommitDisabled;
        return this;
    }

    public MapSchedulerBuilder pollUsingFetchAndLockOnExecute(
            double lowerLimitFractionOfThreads, double executionsPerBatchFractionOfThreads) {
        this.pollingStrategyConfig =
                new PollingStrategyConfig(
                        PollingStrategyConfig.Type.FETCH,
                        lowerLimitFractionOfThreads,
                        executionsPerBatchFractionOfThreads);
        return this;
    }

    public MapSchedulerBuilder pollUsingLockAndFetch(
            double lowerLimitFractionOfThreads, double upperLimitFractionOfThreads) {
        this.pollingStrategyConfig =
                new PollingStrategyConfig(
                        PollingStrategyConfig.Type.LOCK_AND_FETCH,
                        lowerLimitFractionOfThreads,
                        upperLimitFractionOfThreads);
        return this;
    }

    public MapSchedulerBuilder failureLogging(LogLevel logLevel, boolean logStackTrace) {
        if (logLevel == null) {
            throw new IllegalArgumentException("Log level must not be null");
        }
        this.logLevel = logLevel;
        this.logStackTrace = logStackTrace;
        return this;
    }

    public MapSchedulerBuilder registerShutdownHook() {
        this.registerShutdownHook = true;
        return this;
    }

    public MapSchedulerBuilder enablePriority() {
        this.enablePriority = true;
        return this;
    }

    public MapSchedulerBuilder clock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public MapSchedulerBuilder repository(TaskRepository repository) {
        this.taskRepository = repository;
        return this;
    }

    public MapSchedulerBuilder taskResolver(TaskResolver resolver) {
        this.taskResolver = resolver;
        return this;
    }

    public MapScheduler build() {
        if (schedulerName == null) {
            schedulerName = new SchedulerName.Hostname();
        }

        ExecutorService candidateExecutorService = executorService;
        if (candidateExecutorService == null) {
            candidateExecutorService =
                    Executors.newFixedThreadPool(
                            executorThreads, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-"));
        }

        ExecutorService candidateDueExecutor = dueExecutor;
        if (candidateDueExecutor == null) {
            candidateDueExecutor =
                    Executors.newSingleThreadExecutor(
                            defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-execute-due-"));
        }

        ScheduledExecutorService candidateHousekeeperExecutor = housekeeperExecutor;
        if (candidateHousekeeperExecutor == null) {
            candidateHousekeeperExecutor =
                    Executors.newScheduledThreadPool(
                            3, defaultThreadFactoryWithPrefix(THREAD_PREFIX + "-housekeeper-"));
        }

        if (statsRegistry != null) {
            addSchedulerListener(new StatsRegistryAdapter(statsRegistry));
        }

        Waiter waiter = buildWaiter();

        LOG.info(
                "Creating scheduler with configuration: threads={}, pollInterval={}s, heartbeat={}s, enable-immediate-execution={}, enable-priority={}, table-name={}, name={}",
                executorThreads,
                waiter.getWaitDuration().getSeconds(),
                heartbeatInterval.getSeconds(),
                enableImmediateExecution,
                enablePriority,
                tableName,
                schedulerName.getName());

        final MapScheduler scheduler =
                new MapScheduler(
                        clock,
                        taskRepository,
                        taskRepository,
                        taskResolver,
                        executorThreads,
                        candidateExecutorService,
                        schedulerName,
                        waiter,
                        heartbeatInterval,
                        numberOfMissedHeartbeatsBeforeDead,
                        schedulerListeners,
                        executionInterceptors,
                        pollingStrategyConfig,
                        deleteUnresolvedAfter,
                        shutdownMaxWait,
                        logLevel,
                        logStackTrace,
                        startTasks,
                        candidateDueExecutor,
                        candidateHousekeeperExecutor);

        if (enableImmediateExecution) {
//            scheduler.registerSchedulerListener(new ImmediateCheckForDueExecutions(scheduler, clock));
        }

        if (registerShutdownHook) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        LOG.info("Received shutdown signal.");
                                        scheduler.stop();
                                    }));
        }

        return scheduler;
    }

    protected Waiter buildWaiter() {
        return new Waiter(poolingInterval, clock);
    }
}
