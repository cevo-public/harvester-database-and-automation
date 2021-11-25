package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public abstract class StackMachineState {
    public abstract String getElementName();

    /**
     * This function will be called when encountering an opening tag. If a new state is returned, the state will be
     * put on top of the state stack.
     */
    public abstract Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context);

    /**
     * This function will be called when this state was just added to the state stack.
     */
    public abstract void entering(StackMachineContext context);

    /**
     * This function will be called when this state is about to be removed from the state stack.
     */
    public abstract void leaving(StackMachineContext context);
}
