package com.github.kagkarlsson.examples;

import com.github.kagkarlsson.scheduler.Clock;
import com.github.kagkarlsson.scheduler.ScheduledExecutionsFilter;
import com.github.kagkarlsson.scheduler.SystemClock;
import com.github.kagkarlsson.scheduler.TaskRepository;
import com.github.kagkarlsson.scheduler.TaskResolver;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceException;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MapTaskRepository implements TaskRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapTaskRepository.class);

    //table fields - Start
    public static final String TASK_NAME = "task_name";
    public static final String TASK_INSTANCE = "task_instance";
    public static final String TASK_DATA = "task_data";
    public static final String EXECUTION_TIME = "execution_time";
    public static final String PICKED = "picked";
    public static final String VERSION = "version";
    public static final String PRIORITY = "priority";
    //end

    private final Clock clock = new SystemClock();
    private final boolean orderByPriority = false;

    private final TaskResolver taskResolver;

    private final Map<ExecutionKey, Map<String, Object>> store = new ConcurrentHashMap<>();

    public MapTaskRepository(TaskResolver taskResolver) {
        this.taskResolver = taskResolver;
    }

    @Override
    public boolean createIfNotExists(SchedulableInstance<?> instance) {
        return createIfNotExists(
                new ScheduledTaskInstance(
                        instance.getTaskInstance(), instance.getNextExecutionTime(clock.now())));
    }

    @Override
    public boolean createIfNotExists(ScheduledTaskInstance instance) {
        final TaskInstance taskInstance = instance.getTaskInstance();

        Optional<Execution> existingExecution = getExecution(taskInstance);
        if (existingExecution.isPresent()) {
            LOGGER.debug(
                    "Execution not created, it already exists. Due: {}",
                    existingExecution.get().executionTime);
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
                    throw new TaskInstanceException(
                            "Failed to add new execution.", instance.getTaskName(), instance.getId());
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

        String taskName = (String) in.get(TASK_NAME);
//
//        Optional<Task> task = taskResolver.resolve(taskName);
//        Supplier dataSupplier = () -> null;
//        if (task.isPresent()) {
//            dataSupplier = memoize(
//                    () -> serializer.deserialize(task.get().getDataClass(), in.getTaskData()));
//        }
//
//        TaskInstance taskInstance = new TaskInstance(taskName, in.getTaskInstance(), dataSupplier);
//
//        return new Execution(in.getExecutionTime(), taskInstance, in.isPicked(), in.getPickedBy(),
//                        in.getLastSuccess(), in.getLastFailure(), in.getConsecutiveFailures(),
//                        in.getLastHeartbeat(), in.getVersion());

        return null;
    }

    @Override
    public List<Execution> getDue(Instant now, int limit) {
        return store.values().stream().filter(item -> {

            return now.isAfter((Instant) item.get(EXECUTION_TIME))
                    && Boolean.FALSE.equals(item.get(PICKED));

        }).map( item -> {

            return toExecution(item);

        }).collect(Collectors.toList());
    }

    @Override
    public void createBatch(List<ScheduledTaskInstance> executions) {

    }

    @Override
    public Instant replace(Execution toBeReplaced, ScheduledTaskInstance newInstance) {
        return null;
    }

    @Override
    public Instant replace(Execution toBeReplaced, SchedulableInstance<?> newInstance) {
        return null;
    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter filter, Consumer<Execution> consumer) {

    }

    @Override
    public void getScheduledExecutions(ScheduledExecutionsFilter filter, String taskName, Consumer<Execution> consumer) {

    }

    @Override
    public List<Execution> lockAndFetchGeneric(Instant now, int limit) {
        return List.of();
    }

    @Override
    public List<Execution> lockAndGetDue(Instant now, int limit) {
        return List.of();
    }

    @Override
    public void remove(Execution execution) {

    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return false;
    }

    @Override
    public boolean reschedule(Execution execution, Instant nextExecutionTime, Object newData, Instant lastSuccess, Instant lastFailure, int consecutiveFailures) {
        return false;
    }

    @Override
    public Optional<Execution> pick(Execution e, Instant timePicked) {
        return Optional.empty();
    }

    @Override
    public List<Execution> getDeadExecutions(Instant olderThan) {
        return List.of();
    }

    @Override
    public boolean updateHeartbeatWithRetry(Execution execution, Instant newHeartbeat, int tries) {
        return false;
    }

    @Override
    public boolean updateHeartbeat(Execution execution, Instant heartbeatTime) {
        return false;
    }

    @Override
    public List<Execution> getExecutionsFailingLongerThan(Duration interval) {
        return List.of();
    }

    @Override
    public Optional<Execution> getExecution(String taskName, String taskInstanceId) {
        return Optional.empty();
    }

    @Override
    public int removeExecutions(String taskName) {
        return 0;
    }

    @Override
    public void verifySupportsLockAndFetch() {

    }
}