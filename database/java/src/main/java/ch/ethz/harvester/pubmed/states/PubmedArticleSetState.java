package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.PubmedArticle;
import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public class PubmedArticleSetState extends StackMachineState {
    @Override
    public String getElementName() {
        return "PubmedArticleSet";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        if (elementName.equals("PubmedArticle")) {
            return Optional.of(new PubmedArticleState());
        }
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
    }
}
