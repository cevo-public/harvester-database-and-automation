package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.PubmedArticle;
import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public class PubmedArticleState extends StackMachineState {
    @Override
    public String getElementName() {
        return "PubmedArticle";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        if (elementName.equals("MedlineCitation")) {
            return Optional.of(new MedlineCitationState());
        }
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
        context.getArticles().addFirst(new PubmedArticle());
    }

    @Override
    public void leaving(StackMachineContext context) {
    }
}
