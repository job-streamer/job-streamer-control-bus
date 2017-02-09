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

public class JobStreamerExecuteJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap data = context.getMergedJobDataMap();
        String jobName = data.getString("job-name");
        String appName = data.getString("app-name");

        Object system = SystemUtil.getSystem();
        Object jobs = RT.get(system, Keyword.intern("jobs"));

        IFn executionsResource = Clojure.var("job-streamer.control-bus.component.jobs", "executions-resource");
        IFn handler = (IFn) executionsResource.invoke(jobs, appName, jobName);
        PersistentHashMap request = PersistentHashMap.create(
                Keyword.intern("request-method"), Keyword.intern("post"),
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
