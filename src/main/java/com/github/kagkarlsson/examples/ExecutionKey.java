package com.github.kagkarlsson.examples;

public class ExecutionKey {

    private String taskName;
    private String taskInstance;

    public ExecutionKey(String taskName, String taskInstance) {
        this.taskInstance = taskInstance;
        this.taskName = taskName;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getTaskInstance() {
        return taskInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        ExecutionKey key = (ExecutionKey) o;
        return taskName.equals(key.taskName) && taskInstance.equals(key.taskInstance);
    }

    @Override
    public int hashCode() {
        int result = taskName.hashCode();
        result = 31 * result + taskInstance.hashCode();
        return result;
    }
}
