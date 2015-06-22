package net.unit8.job_streamer.control_bus;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

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

        String host = data.getString("host");
        int port = data.getInt("port");

        URLConnection conn;
        try {
            URL url = new URL("http://" + host + ":" + port
                    + "/" + appName
                    + "/job/" + jobName
                    + "/execution/" + executionId
                    + "/" + command);

            conn = url.openConnection();
            conn.setReadTimeout(5000);
            conn.setConnectTimeout(1000);
            ((HttpURLConnection) conn).setRequestMethod("PUT");
            conn.connect();
            int statusCode = ((HttpURLConnection) conn).getResponseCode();
            if (statusCode != HttpURLConnection.HTTP_CREATED) {
                throw new JobExecutionException("HttpRequest not success:" + statusCode);
            }
        } catch (IOException ex) {
            throw new JobExecutionException(ex);
        }


    }
}
