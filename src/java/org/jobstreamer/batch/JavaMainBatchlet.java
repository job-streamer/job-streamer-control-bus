package org.jobstreamer.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.inject.Any;
import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author kawasima
 */
public class JavaMainBatchlet extends AbstractBatchlet {
    private final static Logger logger = LoggerFactory.getLogger(ShellBatchlet.class);

    @Any
    @Inject
    StepContext stepContext;

    @Override
    public String process() throws Exception {
        String main = stepContext.getProperties().getProperty("main");
        if (main == null) {
            logger.error("main is null");
            throw new IllegalStateException("main is null");
        }
        String argString = stepContext.getProperties().getProperty("args");

        Class<?> mainClass = Class.forName(main);
        Method mainMethod = mainClass.getMethod("main", String[].class);
        String[] args = argString.split("\\s+");

        SecurityManager origManager = System.getSecurityManager();
        InterceptExitSecurityManager sm = new InterceptExitSecurityManager();
        System.setSecurityManager(sm);

        try {
            Object[] argsObj = new Object[]{args};
            mainMethod.invoke(null, argsObj);
            return "COMPLETE";
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof SecurityException) {
                Integer status = sm.getExitStatus();
                return Integer.toString(status);
            } else if (ex.getTargetException() instanceof Exception) {
                throw (Exception) ex.getTargetException();
            } else {
                throw (Error) ex.getTargetException();
            }
        } finally {
            System.setSecurityManager(origManager);
        }
    }

    @Override
    public void stop() {
    }
}
