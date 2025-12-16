package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.scheduler.*;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceException;
import com.github.kagkarlsson.scheduler.jdbc.AutodetectJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.*;
import com.github.kagkarlsson.shaded.jdbc.JdbcRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MapTaskRepository implements TaskRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapTaskRepository.class);

    //table fields - Start
    public static final String TASK_NAME = "task_name";
    public static final String TASK_INSTANCE = "task_instance";
    public static final String TASK_DATA = "task_data";
    public static final String EXECUTION_TIME = "execution_time";
    public static final String PICKED = "picked";
    public static final String PICKED_BY = "picked_by";
    public static final String VERSION = "version";
    public static final String PRIORITY = "priority";
    public static final String LAST_SUCCESS = "last_success";
    public static final String LAST_FAILURE = "last_failure";
    public static final String FAILURES = "failures";
    public static final String LAST_HEARTBEAT = "last_heartbeat";
    //end

    private Clock clock = new SystemClock();
    private boolean orderByPriority = false;

    private SchedulerName schedulerSchedulerName;

    private final TaskResolver taskResolver;

    private final Map<ExecutionKey, Map<String, Object>> store = new ConcurrentHashMap<>();

    public MapTaskRepository(TaskResolver taskResolver) {
        this.taskResolver = taskResolver;
    }


    public MapTaskRepository(DataSource dataSource, boolean commitWhenAutocommitDisabled, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, boolean orderByPriority, Clock clock) {
        this(dataSource, commitWhenAutocommitDisabled, new AutodetectJdbcCustomization(dataSource), tableName, taskResolver, schedulerSchedulerName, Serializer.DEFAULT_JAVA_SERIALIZER, orderByPriority, clock);
    }

    public MapTaskRepository(DataSource dataSource, boolean commitWhenAutocommitDisabled, JdbcCustomization jdbcCustomization, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, boolean orderByPriority, Clock clock) {
        this(dataSource, commitWhenAutocommitDisabled, jdbcCustomization, tableName, taskResolver, schedulerSchedulerName, Serializer.DEFAULT_JAVA_SERIALIZER, orderByPriority, clock);
    }

    public MapTaskRepository(DataSource dataSource, boolean commitWhenAutocommitDisabled, JdbcCustomization jdbcCustomization, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, Serializer serializer, boolean orderByPriority, Clock clock) {
        this(jdbcCustomization, tableName, taskResolver, schedulerSchedulerName, serializer, new JdbcRunner(dataSource, commitWhenAutocommitDisabled), orderByPriority, clock);
    }

    protected MapTaskRepository(JdbcCustomization jdbcCustomization, String tableName, TaskResolver taskResolver, SchedulerName schedulerSchedulerName, Serializer serializer, JdbcRunner jdbcRunner, boolean orderByPriority, Clock clock) {
        //this.tableName = tableName;
        this.taskResolver = taskResolver;
        this.schedulerSchedulerName = schedulerSchedulerName;
//        this.jdbcRunner = jdbcRunner;
//        this.serializer = serializer;
//        this.jdbcCustomization = jdbcCustomization;
        this.orderByPriority = orderByPriority;
        this.clock = clock;
//        this.insertQuery = getInsertQuery(tableName);
    }

    @Override
    public boolean createIfNotExists(SchedulableInstance<?> instance) {
        return createIfNotExists(new ScheduledTaskInstance(instance.getTaskInstance(), instance.getNextExecutionTime(clock.now())));
    }

    @Override
    public boolean createIfNotExists(ScheduledTaskInstance instance) {
        final TaskInstance taskInstance = instance.getTaskInstance();

        Optional<Execution> existingExecution = getExecution(taskInstance);
        if (existingExecution.isPresent()) {
            LOGGER.debug("Execution not created, it already exists. Due: {}", existingExecution.get().executionTime);
            return false;
        }

        String taskName = instance.getTaskName();
        String taskInstanceId = taskInstance.getId();

        ExecutionKey key = new ExecutionKey(taskName, taskInstanceId);

        synchronized (store) {
            if (store.get(key) == null) {

                Map<String, Object> taskInstanceMap = new LinkedHashMap<>();
                fillTask(taskInstanceMap, instance);
                store.put(key, taskInstanceMap);

                return true;
            } else {

                LOGGER.debug("Exception when inserting execution. Assuming it to be a constraint violation.");
                existingExecution = getExecution(taskInstance);
                if (existingExecution.isEmpty()) {
                    throw new TaskInstanceException("Failed to add new execution.", instance.getTaskName(), instance.getId());
                }
                LOGGER.debug("Execution not created, another thread created it.");
                return false;

            }
        }
    }

    private void fillTask(Map<String, Object> taskInstanceMap, ScheduledTaskInstance instance) {
        TaskInstance<?> taskInstance = instance.getTaskInstance();

        taskInstanceMap.put(TASK_NAME, taskInstance.getTaskName());
        taskInstanceMap.put(TASK_INSTANCE, taskInstance.getId());
        taskInstanceMap.put(TASK_DATA, taskInstance.getData());
        taskInstanceMap.put(EXECUTION_TIME, instance.getExecutionTime());
        taskInstanceMap.put(PICKED, false);
        taskInstanceMap.put(VERSION, 1L);

        if (orderByPriority) {
            taskInstanceMap.put(PRIORITY, taskInstance.getPriority());
        }
    }

    private Execution toExecution(Map<String, Object> in) {

        if (in == null) return null;

        String taskName = (String) in.get(TASK_NAME);

        Resolvable resolvableTaskName = Resolvable.of(taskName, (Instant) in.get(EXECUTION_TIME));

        Optional<Task> task = taskResolver.resolve(resolvableTaskName);
        Supplier dataSupplier = () -> null;

        TaskInstance taskInstance = new TaskInstance(taskName, (String) in.get(TASK_INSTANCE), dataSupplier);

        return new Execution((Instant) in.getOrDefault(EXECUTION_TIME, Instant.now()), taskInstance, (Boolean) in.getOrDefault(PICKED, false), (String) in.get(PICKED_BY), (Instant) in.get(LAST_SUCCESS), (Instant) in.get(LAST_FAILURE), (Integer) in.get(FAILURES), (Instant) in.get(LAST_HEARTBEAT), (Long) in.get(VERSION));
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        return store.values().stream().filter(item -> {

            return now.isAfter((Instant) item.get(EXECUTION_TIME)) && Boolean.FALSE.equals(item.get(PICKED));

        }).map(item -> {

            return toExecution(item);

        }).collect(Collectors.toList());
    }

    @Override
    public void createBatch(List<ScheduledTaskInstance> executions) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Instant replace(Execution toBeReplaced, ScheduledTaskInstance newInstance) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Instant replace(Execution toBeReplaced, SchedulableInstance<?> newInstance) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter filter, Consumer<Execution> consumer) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter filter, String taskName, Consumer<Execution> consumer) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public List<Execution> lockAndFetchGeneric(Instant now, int limit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public List<Execution> lockAndGetDue(Instant now, int limit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void remove(Execution execution) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Optional<Execution> pick(Execution e, Instant timePicked) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public List<Execution> getDeadExecutions(Instant olderThan) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean updateHeartbeatWithRetry(Execution execution, Instant newHeartbeat, int tries) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean updateHeartbeat(Execution execution, Instant heartbeatTime) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
        Execution execution = toExecution(store.get(new ExecutionKey(taskName, taskInstanceId)));
        return Optional.ofNullable(execution);
    }

    @Override
    public int removeExecutions(String taskName) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void verifySupportsLockAndFetch() {
        throw new RuntimeException("Not implemented");
    }
}