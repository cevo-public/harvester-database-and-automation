package ch.ethz.harvester.pubmed.states;

import ch.ethz.harvester.pubmed.StackMachineContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class ArticleState extends StackMachineState {
    private ValueState<String> articleTitle;
    private List<ValueState<String>> languages = new ArrayList<>();

    @Override
    public String getElementName() {
        return "Article";
    }

    @Override
    public Optional<StackMachineState> consumeBeginElement(String elementName, StackMachineContext context) {
        return Optional.ofNullable(switch (elementName) {
            case "Journal" -> new JournalState();
            case "ArticleTitle" -> articleTitle = new ValueState<>("ArticleTitle", x -> x);
            case "Abstract" -> new AbstractState();
            case "AuthorList" -> new AuthorListState();
            case "Language" -> {
                ValueState<String> newState = new ValueState<>("Language", x -> x);
                languages.add(newState);
                yield newState;
            }
            default -> null;
        });
    }

    @Override
    public void entering(StackMachineContext context) {

    }

    @Override
    public void leaving(StackMachineContext context) {
        if (articleTitle != null) {
            context.getArticles().getFirst().setArticleTitle(articleTitle.getValue());
        }
        if (!languages.isEmpty()) {
            String joinedLanguages = languages.stream()
                    .map(ValueState::getValue)
                    .collect(Collectors.joining(","));
            context.getArticles().getFirst().setLanguage(joinedLanguages);
        }
    }
}
