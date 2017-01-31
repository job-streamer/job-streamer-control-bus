package org.jobstreamer.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.batch.api.AbstractBatchlet;
import javax.batch.runtime.BatchStatus;
import java.util.Random;

/**
 * @author Yuki Seki
 */
public class TestBatchlet extends AbstractBatchlet {
    private static Logger LOG = LoggerFactory.getLogger(TestBatchlet.class);
    @Override
    public String process() throws Exception {
        LOG.info("This is TEST execution");
        return BatchStatus.COMPLETED.name();
    }
}
