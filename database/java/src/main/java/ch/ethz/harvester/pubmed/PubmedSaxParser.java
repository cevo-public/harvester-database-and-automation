package ch.ethz.harvester.pubmed;

import ch.ethz.harvester.pubmed.states.PubmedArticleSetState;
import ch.ethz.harvester.pubmed.states.StackMachineState;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;


public class PubmedSaxParser extends DefaultHandler {

    private final Deque<StackMachineState> stateStack = new LinkedList<>();
    private final StackMachineContext context = new StackMachineContext();

    public PubmedSaxParser() {
        stateStack.addFirst(new PubmedArticleSetState());
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        Optional<StackMachineState> newStateOpt = stateStack.getFirst().consumeBeginElement(qName, context);
        if (newStateOpt.isPresent()) {
            StackMachineState newState = newStateOpt.get();
            newState.entering(context);
            stateStack.addFirst(newState);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        StackMachineState topState = stateStack.getFirst();
        if (topState.getElementName().equals(qName)) {
            topState.leaving(context);
            stateStack.removeFirst();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        context.getValue().append(ch, start, length);
    }

    public StackMachineContext getContext() {
        return context;
    }
}
