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
import javax.swing.text.html.Option;
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

import static com.github.kagkarlsson.examples.MapTaskRepository.Fields.*;
import static com.github.kagkarlsson.scheduler.StringUtils.truncate;

public class MapTaskRepository implements TaskRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapTaskRepository.class);

    //table fields - Start
    public enum Fields {
        TASK_NAME("task_name"),
        TASK_INSTANCE("task_instance"),
        TASK_DATA("task_data"),
        EXECUTION_TIME("execution_time"),
        PICKED("picked"),
        PICKED_BY("picked_by"),
        LAST_SUCCESS("last_success"),
        LAST_FAILURE("last_failure"),
        CONSECUTIVE_FAILURES("consecutive_failures"),
        LAST_HEARTBEAT("last_heartbeat"),
        VERSION("version"),
        PRIORITY("priority");

        private Fields(String fieldName) {
            this.fieldName = fieldName;
        }

        private String fieldName;

        public String getFieldName() {
            return fieldName;
        }

    }

    /*
    task_name            varchar(100),
    task_instance        varchar(100),
    task_data            blob,
    execution_time       TIMESTAMP WITH TIME ZONE,
    picked               BIT,
    picked_by            varchar(50),
    last_success         TIMESTAMP WITH TIME ZONE,
    last_failure         TIMESTAMP WITH TIME ZONE,
    consecutive_failures INT,
    last_heartbeat       TIMESTAMP WITH TIME ZONE,
    version              BIGINT,
    priority             INT,
     */

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

        taskInstanceMap.put(TASK_NAME.getFieldName(), taskInstance.getTaskName());
        taskInstanceMap.put(TASK_INSTANCE.getFieldName(), taskInstance.getId());
        taskInstanceMap.put(TASK_DATA.getFieldName(), taskInstance.getData());
        taskInstanceMap.put(EXECUTION_TIME.getFieldName(), instance.getExecutionTime());
        taskInstanceMap.put(PICKED.getFieldName(), false);
        taskInstanceMap.put(VERSION.getFieldName(), 1L);

        if (orderByPriority) {
            taskInstanceMap.put(PRIORITY.getFieldName(), taskInstance.getPriority());
        }
    }

    private Execution toExecution(Map<String, Object> in) {

        if (in == null) return null;

        String taskName = (String) in.get(TASK_NAME.getFieldName());

        Resolvable resolvableTaskName = Resolvable.of(taskName, (Instant) in.get(EXECUTION_TIME.getFieldName()));

        Optional<Task> task = taskResolver.resolve(resolvableTaskName);
        Supplier dataSupplier = () -> null;

        TaskInstance taskInstance = new TaskInstance(taskName, (String) in.get(TASK_INSTANCE.getFieldName()), dataSupplier);

        /*
        TASK_NAME("task_name"),
        TASK_INSTANCE("task_instance"),
        TASK_DATA("task_data"),
        EXECUTION_TIME("execution_time"),
        PICKED("picked"),
        PICKED_BY("picked_by"),
        LAST_SUCCESS("last_success"),
        LAST_FAILURE("last_failure"),
        CONSECUTIVE_FAILURES("consecutive_failures"),
        LAST_HEARTBEAT("last_heartbeat"),
        VERSION("version"),
        PRIORITY("priority");
         */

        return new Execution(
                (Instant) in.getOrDefault(EXECUTION_TIME.getFieldName(), Instant.now()),
                taskInstance,
                (Boolean) in.getOrDefault(PICKED.getFieldName(), false),
                (String) in.getOrDefault(PICKED_BY.getFieldName(), ""),
                (Instant) in.getOrDefault(LAST_SUCCESS.getFieldName(), Instant.MIN),
                (Instant) in.getOrDefault(LAST_FAILURE.getFieldName(), Instant.MIN),
                (Integer) in.getOrDefault(CONSECUTIVE_FAILURES.getFieldName(), 0),
                (Instant) in.getOrDefault(LAST_HEARTBEAT.getFieldName(), Instant.MIN),
                (Long) in.getOrDefault(VERSION.getFieldName(), 0)
        );
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        return store.values().stream().filter(item -> {

            return now.isAfter((Instant) item.get(EXECUTION_TIME.getFieldName())) && Boolean.FALSE.equals(item.get(PICKED.getFieldName()));

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
        return reschedule(execution, nextExecutionTime, null, lastSuccess, lastFailure, consecutiveFailures);
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {

        synchronized (store) {
            ExecutionKey key = new ExecutionKey(execution.getTaskName(), execution.taskInstance.getId());

            Map<String, Object> item = store.get(key);

            if ((Long) item.getOrDefault(VERSION.getFieldName(), 0L) == execution.version) {
                item.put(PICKED.getFieldName(), false);
                item.put(PICKED_BY.getFieldName(), "");
                item.put(LAST_HEARTBEAT.getFieldName(), Instant.MIN);
                item.put(LAST_SUCCESS.getFieldName(), lastSuccess);
                item.put(LAST_FAILURE.getFieldName(), lastFailure);
                item.put(CONSECUTIVE_FAILURES.getFieldName(), consecutiveFailures);
                item.put(EXECUTION_TIME.getFieldName(), nextExecutionTime);
                if (newData != null) {
                    item.put(TASK_DATA.getFieldName(), newData);
                }
                item.put(VERSION.getFieldName(), ((Long) item.getOrDefault(VERSION.getFieldName(), 0)) + 1L);

                return true;
            } else {
                return false;
            }

        }
    }

    @Override
    public Optional<Execution> pick(Execution e, Instant timePicked) {

        synchronized (store) {
            ExecutionKey key = new ExecutionKey(e.getTaskName(), e.taskInstance.getId());
            Map<String, Object> execution = store.get(key);

            if ((Boolean) execution.getOrDefault(PICKED.getFieldName(), false) == false) {
                if ((Long)execution.getOrDefault(VERSION.getFieldName(), 0L) == e.version) {
                    execution.put(PICKED.getFieldName(), true);
                    execution.put(PICKED_BY.getFieldName(), truncate(schedulerSchedulerName.getName(), 50));
                    execution.put(LAST_HEARTBEAT.getFieldName(), timePicked);
                    execution.put(VERSION.getFieldName(), ((Long) execution.getOrDefault(VERSION.getFieldName(), 0)) + 1L);

                    return Optional.of(toExecution(execution));
                }
            }

            return Optional.empty();
        }
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