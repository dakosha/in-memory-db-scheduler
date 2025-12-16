package com.github.kagkarlsson.examples;

import java.time.Instant;
import java.util.Objects;

public class ExecutionRecord {

    final String taskName;
    final String taskInstance;

    volatile Instant executionTime;
    volatile boolean picked;
    volatile String pickedBy;
    volatile Instant lastHeartbeat;
    volatile Instant lastSuccess;
    volatile Instant lastFailure;
    volatile int consecutiveFailures;
    volatile long version;
    volatile int priority;

    final byte[] taskData;

    ExecutionRecord(
            String taskName,
            String taskInstance,
            Instant executionTime,
            byte[] taskData,
            int priority
    ) {
        this.taskName = taskName;
        this.taskInstance = taskInstance;
        this.executionTime = executionTime;
        this.taskData = taskData;
        this.priority = priority;
        this.picked = false;
        this.version = 1L;
    }

    ExecutionKey key() {
        return new ExecutionKey(taskName, taskInstance);
    }
}