package ch.ethz.harvester.pubmed.states;


import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;
import java.util.function.Function;


public class ValueState<T> extends StackMachineState {
    private final String fieldName;
    private final Function<String, T> parseFunction;
    private T value;

    public ValueState(String fieldName, Function<String, T> parseFunction) {
        this.fieldName = fieldName;
        this.parseFunction = parseFunction;
    }

    @Override
    public String getElementName() {
        return fieldName;
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
        context.getValue().setLength(0);
    }

    @Override
    public void leaving(StackMachineContext context) {
        this.value = parseFunction.apply(context.getValue().toString().trim());
    }

    public T getValue() {
        return value;
    }
}
