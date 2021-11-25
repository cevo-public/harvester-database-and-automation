package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public class JournalState extends StackMachineState {
    private ValueState<String> title;

    @Override
    public String getElementName() {
        return "Journal";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        if (elementName.equals("Title")) {
            title = new ValueState<>("Title", x -> x);
            return Optional.of(title);
        }
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        if (title != null) {
            context.getArticles().getFirst().setJournalTitle(title.getValue());
        }
    }
}
