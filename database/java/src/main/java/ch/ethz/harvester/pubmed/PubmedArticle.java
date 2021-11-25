package ch.ethz.harvester.pubmed;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;


public class PubmedArticle {

    private long pmid;
    private LocalDate dateCompleted;
    private LocalDate dateRevised;
    private String articleTitle;
    private String journalTitle;
    private String articleAbstract;
    private List<PubmedAuthor> authors = new ArrayList<>();
    private String language;

    public long getPmid() {
        return pmid;
    }

    public PubmedArticle setPmid(long pmid) {
        this.pmid = pmid;
        return this;
    }

    public LocalDate getDateCompleted() {
        return dateCompleted;
    }

    public PubmedArticle setDateCompleted(LocalDate dateCompleted) {
        this.dateCompleted = dateCompleted;
        return this;
    }

    public LocalDate getDateRevised() {
        return dateRevised;
    }

    public PubmedArticle setDateRevised(LocalDate dateRevised) {
        this.dateRevised = dateRevised;
        return this;
    }

    public String getArticleTitle() {
        return articleTitle;
    }

    public PubmedArticle setArticleTitle(String articleTitle) {
        this.articleTitle = articleTitle;
        return this;
    }

    public String getJournalTitle() {
        return journalTitle;
    }

    public PubmedArticle setJournalTitle(String journalTitle) {
        this.journalTitle = journalTitle;
        return this;
    }

    public String getArticleAbstract() {
        return articleAbstract;
    }

    public PubmedArticle setArticleAbstract(String articleAbstract) {
        this.articleAbstract = articleAbstract;
        return this;
    }

    public List<PubmedAuthor> getAuthors() {
        return authors;
    }

    public PubmedArticle setAuthors(List<PubmedAuthor> authors) {
        this.authors = authors;
        return this;
    }

    public String getLanguage() {
        return language;
    }

    public PubmedArticle setLanguage(String language) {
        this.language = language;
        return this;
    }
}
