package ch.ethz.harvester.rxiv;

import java.time.LocalDate;
import java.util.List;


public class RxivArticle {

    private String doi;
    private String title;
    private List<String> authors;
    private LocalDate date;
    private int version;
    private String type;
    private String license;
    private String category;
    private String jatsxmlUrl;
    private String abstractText;
    private String published;
    private String server;

    public String getDoi() {
        return doi;
    }

    public RxivArticle setDoi(String doi) {
        this.doi = doi;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public RxivArticle setTitle(String title) {
        this.title = title;
        return this;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public RxivArticle setAuthors(List<String> authors) {
        this.authors = authors;
        return this;
    }

    public LocalDate getDate() {
        return date;
    }

    public RxivArticle setDate(LocalDate date) {
        this.date = date;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public RxivArticle setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getType() {
        return type;
    }

    public RxivArticle setType(String type) {
        this.type = type;
        return this;
    }

    public String getLicense() {
        return license;
    }

    public RxivArticle setLicense(String license) {
        this.license = license;
        return this;
    }

    public String getCategory() {
        return category;
    }

    public RxivArticle setCategory(String category) {
        this.category = category;
        return this;
    }

    public String getJatsxmlUrl() {
        return jatsxmlUrl;
    }

    public RxivArticle setJatsxmlUrl(String jatsxmlUrl) {
        this.jatsxmlUrl = jatsxmlUrl;
        return this;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public RxivArticle setAbstractText(String abstractText) {
        this.abstractText = abstractText;
        return this;
    }

    public String getPublished() {
        return published;
    }

    public RxivArticle setPublished(String published) {
        this.published = published;
        return this;
    }

    public String getServer() {
        return server;
    }

    public RxivArticle setServer(String server) {
        this.server = server;
        return this;
    }
}
