package net.unit8.job_streamer.control_bus;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import clojure.java.api.Clojure;
import clojure.lang.IFn;
import datomic.Connection;
import datomic.Peer;
import datomic.Util;

import java.util.*;

/**
 * @author kawasima
 */
public class DatomicAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    protected String uri;
    private IFn findStepExecutionFn = Clojure.var("job-streamer.control-bus.job", "find-step-execution");
    @Override
    public void start() {
        super.start();
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (event.getMDCPropertyMap().get("stepExecutionId") == null) return;

        UUID instanceId = UUID.fromString(event.getMDCPropertyMap().get("instanceId"));
        Long stepExecutionId = Long.parseLong(event.getMDCPropertyMap().get("stepExecutionId"));

        Connection conn = Peer.connect(uri);
        Long agentId = Peer.query("[:find ?a . :in $ ?inst-id :where [?a :agent/instance-id ?inst-id]]]"
                , conn.db(), instanceId);

        if (agentId == null) return;
        Object tempId = Peer.tempid(":db.part/user");
        Map executionLog = new HashMap(Util.map(
                ":db/id", tempId,
                ":execution-log/step-execution-id", stepExecutionId,
                ":execution-log/agent", agentId,
                ":execution-log/date", new Date(event.getTimeStamp()),
                ":execution-log/logger", event.getLoggerName(),
                ":execution-log/level", ":log-level/" + event.getLevel().toString().toLowerCase(),
                ":execution-log/message", event.getFormattedMessage()));
        if (event.getThrowableProxy() != null) {
            executionLog.put(
                    ":execution-log/exception",
                    ThrowableProxyUtil.asString(event.getThrowableProxy()));
        }
        List tx = Util.list(executionLog);

        try {
            conn.transact(tx).get();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
