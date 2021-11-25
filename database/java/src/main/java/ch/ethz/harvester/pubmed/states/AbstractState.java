package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class AbstractState extends StackMachineState {
    private final List<ValueState<String>> abstractParts = new ArrayList<>();

    @Override
    public String getElementName() {
        return "Abstract";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        if (elementName.equals("AbstractText")) {
            ValueState<String> newState = new ValueState<>("AbstractText", x -> x);
            abstractParts.add(newState);
            return Optional.of(newState);
        }
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        if (!abstractParts.isEmpty()) {
            String joinedAbstract = abstractParts.stream()
                    .map(ValueState::getValue)
                    .collect(Collectors.joining("\n\n"));
            context.getArticles().getFirst().setArticleAbstract(joinedAbstract);
        }
    }
}
