package ch.ethz.harvester.pubmed.actions;


import ch.ethz.harvester.pubmed.states.StackMachineState;

public class StackMachinePushAction implements StackMachineAction {

    private final StackMachineState state;

    public StackMachinePushAction(StackMachineState state) {
        this.state = state;
    }

    public StackMachineState getState() {
        return state;
    }
}
