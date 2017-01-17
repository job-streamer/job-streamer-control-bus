package net.unit8.job_streamer.control_bus.bpmn;

import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.parser.Tag;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author kawasima
 */
public class BpmnStructureVisitor implements NodeVisitor {
    private static final Set<String> TARGET_TAGS = new HashSet<>(
            Arrays.asList("jsr352:job", "jsr352:batchlet", "jsr352:chunk",
                    "jsr352:writer", "jsr352:reader", "jsr352:processor",
                    "jsr352:step", "jsr352:flow", "jsr352:split")
    );

    private Element current;
    private Map<String, Element> transitions;
    private Map<String, Element> batchComponents;
    private Map<String, Element> endEvents;

    public BpmnStructureVisitor(Element root,
                                Map<String, Element> transitions,
                                Map<String, Element> batchComponents,
                                Map<String, Element> endEvents) {
        current = root;
        this.transitions = transitions;
        this.endEvents = endEvents;
        this.batchComponents = batchComponents;
    }

    private void parseProperties(Element bpmnEl, Element jobEl) {
        Elements properties = bpmnEl.select("> bpmn|extensionElements camunda|property");
        if (properties.isEmpty()) return;

        Element propertiesEl = jobEl.appendElement("properties");
        for (Element property : properties) {
            propertiesEl.appendElement("property")
                    .attr("name",  property.attr("name"))
                    .attr("value", property.attr("value"));
        }
    }

    private void parseListeners(Element bpmnEl, Element jobEl) {
        Elements listeners = bpmnEl.select("> jsr352|listener");
        if (listeners.isEmpty()) return;

        Element listenersEL = jobEl.appendElement("listeners");
        for (Element listener : listeners) {
            Element listenerEl = listenersEL.appendElement("listener")
                    .attr("ref",  listener.attr("ref"));
            parseProperties(listener, listenerEl);
        }
    }

    private void parseTransition(Element bpmnEl, Element jobEl) {
        for (Element outgoing : bpmnEl.select("bpmn|outgoing")) {
            Element transition = transitions.get(outgoing.text().trim());
            String on = transition.hasAttr("on") ? transition.attr("on") : "*";
            String targetRef = transition.attr("targetRef");
            if (endEvents.containsKey(targetRef)) {
                Element endEvent = endEvents.get(targetRef);
                switch (endEvent.nodeName()) {
                    case "jsr352:end":
                        Element endEl = jobEl.appendElement("end");
                        endEl.attr("on", on);
                        if (endEvent.hasAttr("exit-status"))
                                endEl.attr("exit-status", endEvent.attr("exit-status"));
                        break;
                    case "jsr352:fail":
                        Element failEl = jobEl.appendElement("fail");
                        failEl.attr("on", on);
                        if (endEvent.hasAttr("exit-status"))
                            failEl.attr("exit-status", endEvent.attr("exit-status"));
                        break;
                    case "jsr352:stop":
                        Element stopEl = jobEl.appendElement("stop");
                        stopEl.attr("on", on);
                        if (endEvent.hasAttr("exit-status"))
                            stopEl.attr("exit-status", endEvent.attr("exit-status"));
                        break;
                }
            } else if (batchComponents.containsKey(targetRef)) {
                Element nextEl = jobEl.appendElement("next");
                nextEl.attr("on", on);
                nextEl.attr("to", batchComponents.get(targetRef).attr("name"));
            }
        }
    }

    private void copyAttribute(Element el, Element bpmnEl, String attrName) {
        copyAttribute(el, bpmnEl, attrName, null);
    }

    private void copyAttribute(Element el, Element bpmnEl, String attrName, String defaultValue) {
        if (bpmnEl.hasAttr(attrName)) {
            el.attr(attrName, bpmnEl.attr(attrName));
        } else if (defaultValue != null) {
            el.attr(attrName, defaultValue);
        }
    }

    @Override
    public void head(Node node, int i) {
        Element el;
        switch(node.nodeName()) {
            case "jsr352:job":
                el = new Element(Tag.valueOf("job"), "");
                el.attr("id", or(
                        node.attr("name"),node.attr("bpmn:name")));
                parseProperties((Element) node, el);
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:step":
                el = current.appendElement("step");
                el.attr("id", or(
                        node.attr("name"),
                        node.attr("id")
                ));
                copyAttribute(el, (Element) node, "start-limit");
                copyAttribute(el, (Element) node, "allow-start-if-complete");
                parseProperties((Element) node, el);
                parseListeners((Element) node, el);
                parseTransition((Element) node, el);
                current = el;
                break;

            case "jsr352:chunk":
                el = current.appendElement("chunk");
                copyAttribute(el, (Element) node, "checkpoint-policy");
                copyAttribute(el, (Element) node, "item-count");
                copyAttribute(el, (Element) node, "time-limit");
                copyAttribute(el, (Element) node, "skip-limit");
                copyAttribute(el, (Element) node, "retry-limit");
                parseProperties((Element) node, el);
                current = el;
                break;
            case "jsr352:flow":
                el = current.appendElement("flow")
                        .attr("id", or(
                                node.attr("name"),
                                node.attr("id")
                        ));
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:split":
                el = current.appendElement("split")
                        .attr("id", or(
                                node.attr("name"),
                                node.attr("id")
                        ));
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:batchlet":
                el = new Element(Tag.valueOf("batchlet"), "");
                el.attr("ref", node.attr("ref"));
                parseProperties((Element) node, el);
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:reader":
                el = new Element(Tag.valueOf("reader"), "");
                el.attr("ref", node.attr("ref"));
                parseProperties((Element) node, el);
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:processor":
                el = new Element(Tag.valueOf("processor"), "");
                el.attr("ref", node.attr("ref"));
                parseProperties((Element) node, el);
                current.appendChild(el);
                current = el;
                break;
            case "jsr352:writer":
                el = new Element(Tag.valueOf("writer"), "");
                el.attr("ref", node.attr("ref"));
                parseProperties((Element) node, el);
                current.appendChild(el);
                current = el;
                break;
            default:
        }
    }

    @Override
    public void tail(Node node, int i) {
        if (TARGET_TAGS.contains(node.nodeName())) {
            current = current.parent();
        }
    }

    private String or(String... args) {
        for(String arg : args) {
            if (arg != null && !arg.isEmpty())
                return arg;
        }
        return null;
    }
}
