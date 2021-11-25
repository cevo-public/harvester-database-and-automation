package ch.ethz.harvester.pubmed;

import java.util.Deque;
import java.util.LinkedList;


public class StackMachineContext {

    private final StringBuilder value = new StringBuilder();
    private final Deque<PubmedArticle> articles = new LinkedList<>();
    private final Deque<Object> workStack = new LinkedList<>();

    public StringBuilder getValue() {
        return value;
    }

    public Deque<PubmedArticle> getArticles() {
        return articles;
    }

    public Deque<Object> getWorkStack() {
        return workStack;
    }
}
