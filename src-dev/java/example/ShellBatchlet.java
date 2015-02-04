package example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.StepContext;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 * @author kawasima
 */
public class ShellBatchlet extends AbstractBatchlet {
    private static final Logger logger = LoggerFactory.getLogger(ShellBatchlet.class);

    @Inject
    StepContext stepContext;

    @Override
    public String process() throws Exception {
        String script = stepContext.getProperties().getProperty("script");
        if (script == null) {
            logger.error("script is null");
            throw new IllegalStateException("script is null");
        }

        File scriptFile = new File(script);
        if (!scriptFile.exists()) {
            logger.error("script [" + scriptFile + "] is not found");
            throw new IllegalStateException("script is not found.");
        }
        if (!scriptFile.canExecute()) {
            logger.error("script [" + scriptFile + "] is not executable.");
            throw new IllegalStateException("script is not executable.");
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
