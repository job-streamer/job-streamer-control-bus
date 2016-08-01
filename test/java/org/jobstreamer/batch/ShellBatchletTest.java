package org.jobstreamer.batch;

import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Paths;
import java.util.Properties;

import javax.batch.runtime.context.StepContext;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import ch.qos.logback.classic.Logger;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ShellBatchlet.class,Logger.class})
public class ShellBatchletTest {
	private static final String BAT_FILE_PATH = "test/resources/test.bat";
	private static final String SHELL_FILE_PATH = "test/resources/test";
    private static final String TEST_MESSAGE = "test";
    private final static ShellBatchlet TARGET = new ShellBatchlet();

    private StepContext sc = mock (StepContext.class);
    private Properties properties = new Properties();
    private Logger logger = Mockito.mock(Logger.class);
    private boolean isWindows = System.getProperty("os.name").startsWith("Windows");
    private String fileName;

    @Before
    public void setup() throws Throwable{
         properties = new Properties();
         Field f= TARGET.getClass().getDeclaredField("stepContext");
         f.set(TARGET, sc);
         setFinalStatic(ShellBatchlet.class.getDeclaredField("logger"),logger);
         fileName =isWindows ? BAT_FILE_PATH : SHELL_FILE_PATH;

    }
    
    @Test
    public void args0() throws Throwable{
        when(sc.getProperties()).thenReturn(properties);
        TARGET.executeScript(Paths.get(fileName));
        verify(logger,never()).info(TEST_MESSAGE);




    }

    @Test
    public void args1() throws Throwable{
        properties.setProperty("args", TEST_MESSAGE);
        when(sc.getProperties()).thenReturn(properties);
        TARGET.executeScript(Paths.get(fileName));

        verify(logger,times(1)).info(TEST_MESSAGE);
    }

    @Test
    public void args2() throws Throwable{
        properties.setProperty("args", TEST_MESSAGE + " " + TEST_MESSAGE);
        when(sc.getProperties()).thenReturn(properties);
        TARGET.executeScript(Paths.get(fileName));

        verify(logger,times(2)).info(TEST_MESSAGE);
    }

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}
