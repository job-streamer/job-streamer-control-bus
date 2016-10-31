package org.jobstreamer.batch;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.is;

/**
 * @author kawasima
 */
public class JavaMainBatchletTest {
    @Test
    public void wrapJavaMain() throws Exception {
        JavaMainBatchlet batchlet = new JavaMainBatchlet();
        MockStepContext ctx = new MockStepContext();
        ctx.setProperty("main", "org.jobstreamer.batch.MainBatch");
        ctx.setProperty("args", "a b");

        Field stepContextField = JavaMainBatchlet.class.getDeclaredField("stepContext");
        stepContextField.set(batchlet, ctx);
        Assert.assertThat(batchlet.process(), is("10"));
    }

    @Test
    public void wrapClojureMain() throws Exception {
        JavaMainBatchlet batchlet = new JavaMainBatchlet();
        MockStepContext ctx = new MockStepContext();
        ctx.setProperty("main", "clojure.main");
        ctx.setProperty("args", "-m test-job");

        Field stepContextField = JavaMainBatchlet.class.getDeclaredField("stepContext");
        stepContextField.set(batchlet, ctx);
        Assert.assertThat(batchlet.process(), is("COMPLETE"));
    }
}
