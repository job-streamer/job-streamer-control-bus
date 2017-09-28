package net.unit8.job_streamer.control_bus.bpmn;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities;
import org.jsoup.nodes.Node;
import org.jsoup.parser.ParseSettings;
import org.jsoup.parser.Parser;
import org.jsoup.parser.XmlTreeBuilder;
import org.jsoup.select.NodeVisitor;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

/**
 * Parse BPMN file.
 *
 * @author kawasima
 */
public class BpmnParser {
    public BpmnParser() {

    }

    public Document parse(String xml) {
        Parser parser = new Parser(new XmlTreeBuilder())
                .settings(ParseSettings.preserveCase);
        Document doc = parser.parseInput(xml, "");

        final Map<String, Element> batchComponents = new HashMap<>();
        final Map<String, Element> transitions = new HashMap<>();
        final Map<String, Element> endEvents = new HashMap<>();
        final List<String> startBatchIds = new ArrayList<>();

        doc.traverse(new NodeVisitor() {
            @Override
            public void head(Node node, int depth) {
                switch(node.nodeName()) {
                    case "jsr352:step":
                    case "jsr352:flow":
                    case "jsr352:split":
                        batchComponents.put(node.attr("id"), (Element) node);
                        break;
                    case "jsr352:transition":
                        transitions.put(node.attr("id"), (Element) node);
                        break;
                    case "jsr352:end":
                    case "jsr352:fail":
                    case "jsr352:stop":
                        endEvents.put(node.attr("id"), (Element) node);
                        break;
                    default:
                }
            }

            @Override
            public void tail(Node node, int depth) {

            }
        });

        for (Element outgoing : doc.select("jsr352|start > bpmn|outgoing")) {
            Element transition = transitions.get(outgoing.text().trim());
            startBatchIds.add(transition.attr("targetRef"));
        }

        Document root = new Document("");
        root.outputSettings().syntax(Document.OutputSettings.Syntax.xml);

        doc.traverse(new BpmnStructureVisitor(root, transitions, batchComponents, endEvents, startBatchIds));
        return root;
    }
}
