package ch.ethz.harvester.pubmed;


import java.util.Objects;

public class PubmedAuthor {
    private Long id = null;
    private String lastName;
    private String foreName;
    private String collectiveName;

    public Long getId() {
        return id;
    }

    public PubmedAuthor setId(Long id) {
        this.id = id;
        return this;
    }

    public String getLastName() {
        return lastName;
    }

    public PubmedAuthor setLastName(String lastName) {
        this.lastName = lastName;
        return this;
    }

    public String getForeName() {
        return foreName;
    }

    public PubmedAuthor setForeName(String foreName) {
        this.foreName = foreName;
        return this;
    }

    public String getCollectiveName() {
        return collectiveName;
    }

    public PubmedAuthor setCollectiveName(String collectiveName) {
        this.collectiveName = collectiveName;
        return this;
    }

    /**
     * Equals and hashCode do not use the id field.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PubmedAuthor that = (PubmedAuthor) o;
        return Objects.equals(lastName, that.lastName)
                && Objects.equals(foreName, that.foreName)
                && Objects.equals(collectiveName, that.collectiveName);
    }

    /**
     * Equals and hashCode do not use the id field.
     */
    @Override
    public int hashCode() {
        return Objects.hash(lastName, foreName, collectiveName);
    }
}
