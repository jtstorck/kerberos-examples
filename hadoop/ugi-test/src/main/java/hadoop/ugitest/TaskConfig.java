package hadoop.ugitest;

import com.google.common.base.MoreObjects;

public class TaskConfig {
    private final String keytabPath;
    private final String principal;
    private final long initialTaskDelay;
    private final long reloginPeriod;
    private final long taskPeriod;
    private final String destinationPath;

    public TaskConfig(String keytabPath, String principal, long initialTaskDelay, long reloginPeriod, long taskPeriod, String destinationPath) {
        this.keytabPath = keytabPath;
        this.principal = principal;
        this.initialTaskDelay = initialTaskDelay;
        this.reloginPeriod = reloginPeriod;
        this.taskPeriod = taskPeriod;
        this.destinationPath = destinationPath;

    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public String getPrincipal() {
        return principal;
    }

    public long getInitialTaskDelay() {
        return initialTaskDelay;
    }

    public long getReloginPeriod() {
        return reloginPeriod;
    }

    public long getTaskPeriod() {
        return taskPeriod;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("keytabPath", keytabPath)
                .add("principal", principal)
                .add("initialTaskDelay", initialTaskDelay)
                .add("reloginPeriod", reloginPeriod)
                .add("taskPeriod", taskPeriod)
                .add("destinationPath", destinationPath)
                .toString();
    }
}
