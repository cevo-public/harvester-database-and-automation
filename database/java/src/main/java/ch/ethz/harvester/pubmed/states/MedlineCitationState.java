package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public class MedlineCitationState extends StackMachineState {
    private ValueState<Long> pmid;

    @Override
    public String getElementName() {
        return "MedlineCitation";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        StackMachineState state = switch (elementName) {
            case "PMID" -> pmid = new ValueState<>("PMID", Long::parseLong);
            case "DateCompleted" -> new DateCompletedState();
            case "DateRevised" -> new DateRevisedState();
            case "Article" -> new ArticleState();
            default -> null;
        };
        return Optional.ofNullable(state);
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        if (pmid != null) {
            context.getArticles().getFirst().setPmid(pmid.getValue());
        }
    }
}
