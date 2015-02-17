package example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author kawasima
 */
public class ShellBatchlet extends AbstractBatchlet {
    private static final Logger logger = LoggerFactory.getLogger("job-streamer");

    @Inject
    StepContext stepContext;

    @Override
    public String process() throws Exception {
        String script = stepContext.getProperties().getProperty("script");
        if (script == null) {
            logger.error("script is null");
            throw new IllegalStateException("script is null");
        }

        URL resourceUrl = getClass().getClassLoader().getResource(script);
        if (resourceUrl == null) {
            logger.error("resource [" + script + "] is not found.");
            throw new IllegalStateException("resource [" + script + "] is not found.");
        }

        URLConnection connection = resourceUrl.openConnection();
        try (InputStream in = connection.getInputStream()) {
            File scriptFile = File.createTempFile(Paths.get(script).getFileName().toString(), "exe");
            if (!scriptFile.exists()) {
                logger.error("script [" + scriptFile + "] is not found");
                throw new IllegalStateException("script is not found.");
            }
            Files.copy(in, scriptFile.toPath());
            scriptFile.setExecutable(true);
        }

        ProcessBuilder pb = new ProcessBuilder(script);
        pb.redirectErrorStream(true);
        Process process = null;
        try {
            process = pb.start();

            try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = in.readLine()) != null) {
                    logger.info(line);
                }
            }
        } finally {
            if (process != null) {
                return Integer.toString(process.exitValue());
            } else {
                throw new IllegalStateException("process won't start.");
            }
        }
    }
}
