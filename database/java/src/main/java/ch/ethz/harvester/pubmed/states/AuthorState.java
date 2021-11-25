package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.PubmedAuthor;
import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.Optional;


public class AuthorState extends StackMachineState {
    private PubmedAuthor author = new PubmedAuthor();
    private ValueState<String> lastName;
    private ValueState<String> foreName;
    private ValueState<String> collectiveName;

    @Override
    public String getElementName() {
        return "Author";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        return Optional.ofNullable(switch (elementName) {
            case "LastName" -> lastName = new ValueState<>("LastName", x -> x);
            case "ForeName" -> foreName = new ValueState<>("ForeName", x -> x);
            case "CollectiveName" -> collectiveName = new ValueState<>("CollectiveName", x -> x);
            default -> null;
        });
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        if (lastName != null) {
            author.setLastName(lastName.getValue());
        }
        if (foreName != null) {
            author.setForeName(foreName.getValue());
        }
        if (collectiveName != null) {
            author.setCollectiveName(collectiveName.getValue());
        }
    }

    public PubmedAuthor getAuthor() {
        return author;
    }
}
