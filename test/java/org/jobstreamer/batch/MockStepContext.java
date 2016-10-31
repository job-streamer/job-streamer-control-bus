package org.jobstreamer.batch;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.Metric;
import javax.batch.runtime.context.StepContext;
import java.io.Serializable;
import java.util.Properties;

/**
 * @author kawasima
 */
public class MockStepContext implements StepContext {
    private String stepName;
    private Object transientUserData;
    private long stepExecutionId;
    private Properties properties = new Properties();
    private Serializable persistentUserData;
    private BatchStatus batchStatus;
    private String exitStatus;
    private Exception exception;

    @Override
    public String getStepName() {
        return stepName;
    }

    @Override
    public Object getTransientUserData() {
        return transientUserData;
    }

    @Override
    public void setTransientUserData(Object data) {
        this.transientUserData = data;
    }

    @Override
    public long getStepExecutionId() {
        return stepExecutionId;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public void setProperty(String name, String value) {
        properties.setProperty(name, value);
    }

    @Override
    public Serializable getPersistentUserData() {
        return persistentUserData;
    }

    @Override
    public void setPersistentUserData(Serializable data) {
        this.persistentUserData = data;
    }

    @Override
    public BatchStatus getBatchStatus() {
        return batchStatus;
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    @Override
    public void setExitStatus(String status) {
        this.exitStatus = status;
    }

    @Override
    public Exception getException() {
        return exception;
    }

    @Override
    public Metric[] getMetrics() {
        return new Metric[0];
    }
}
