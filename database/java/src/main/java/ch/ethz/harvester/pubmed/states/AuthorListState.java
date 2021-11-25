package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class AuthorListState extends StackMachineState {
    private final List<AuthorState> authors = new ArrayList<>();

    @Override
    public String getElementName() {
        return "AuthorList";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        if (elementName.equals("Author")) {
            AuthorState newState = new AuthorState();
            authors.add(newState);
            return Optional.of(newState);
        }
        return Optional.empty();
    }

    @Override
    public void entering(StackMachineContext context) {
    }

    @Override
    public void leaving(StackMachineContext context) {
        context.getArticles().getFirst()
                .setAuthors(authors.stream()
                        .map(AuthorState::getAuthor)
                        .collect(Collectors.toList()));
    }
}
