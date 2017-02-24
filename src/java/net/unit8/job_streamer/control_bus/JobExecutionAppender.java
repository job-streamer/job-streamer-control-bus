package net.unit8.job_streamer.control_bus;

import clojure.lang.*;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import datomic.Connection;
import datomic.Peer;
import datomic.Util;

import java.util.*;
import net.unit8.job_streamer.control_bus.util.SystemUtil;

/**
 * @author Yuki Seki
 */
public class JobExecutionAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    protected String uri;
    @Override
    public void start() {
        super.start();
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event.getMDCPropertyMap().get("jobExecutionId") == null) return;
        Long jobExecutionId = Long.parseLong(event.getMDCPropertyMap().get("jobExecutionId"));

        Object system = SystemUtil.getSystem();
        Object jobs = RT.get(system, Keyword.intern("jobs"));
        IFn writeErrorMessageOnState = Clojure.var("job-streamer.control-bus.component.jobs", "write-error-message-on-state");
        writeErrorMessageOnState.invoke(jobs, jobExecutionId, event.getMessage(), ThrowableProxyUtil.asString(event.getThrowableProxy()));
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
