package net.unit8.job_streamer.control_bus.bpmn;

import org.apache.commons.io.FileUtils;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author kawasima
 */
public class BpmnParserTest {
    @Test
    public void test() throws IOException {
        String bpmn = FileUtils.readFileToString(new File("dev-resources/job.bpmn"));
        BpmnParser parser = new BpmnParser();
        Document doc = parser.parse(bpmn);
        System.out.println(doc);
    }
}
