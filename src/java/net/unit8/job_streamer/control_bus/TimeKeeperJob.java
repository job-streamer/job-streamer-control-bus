package net.unit8.job_streamer.control_bus;

import clojure.java.api.Clojure;
import clojure.lang.*;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.net.HttpURLConnection;

import net.unit8.job_streamer.control_bus.util.SystemUtil;

/**
 * @author kawasima
 */
public class TimeKeeperJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap data = context.getMergedJobDataMap();
        String appName = data.getString("app-name");
        String jobName = data.getString("job-name");
        Long executionId = data.getLong("execution-id");
        String command = data.getString("command");

        Object system = SystemUtil.getSystem();
        Object jobs = RT.get(system, Keyword.intern("jobs"));

        IFn executionResource = Clojure.var("job-streamer.control-bus.component.jobs", "execution-resource");
        IFn handler = (IFn) executionResource.invoke(jobs, executionId, Keyword.intern(command));
        PersistentHashMap request = PersistentHashMap.create(
                Keyword.intern("request-method"), Keyword.intern("put"),
                Keyword.intern("identity"), PersistentHashMap.create(
                    Keyword.intern("permissions"), PersistentHashSet.create(
                        Keyword.intern("permission", "execute-job"))),
                Keyword.intern("content-type"), "application/edn");
        long statusCode = (long) RT.get(handler.invoke(request), Keyword.intern("status"));
        if (statusCode != (long) HttpURLConnection.HTTP_CREATED) {
            throw new JobExecutionException("HttpRequest not success:" + statusCode);
        }
    }
}
