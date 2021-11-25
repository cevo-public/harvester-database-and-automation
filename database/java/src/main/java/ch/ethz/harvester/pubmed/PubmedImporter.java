package ch.ethz.harvester.pubmed;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.EmptyConfig;
import ch.ethz.harvester.core.SubProgram;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.logging.log4j.util.Strings;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


/**
 * Downloads data from NLM/PubMed
 * Terms and Conditions: https://www.nlm.nih.gov/databases/download/terms_and_conditions_pubmed.html
 */
public class PubmedImporter extends SubProgram<EmptyConfig> {

    private ComboPooledDataSource databasePool;


    public PubmedImporter() {
        super("PubmedImporter", EmptyConfig.class);
    }


    @Override
    public void run(String[] args, EmptyConfig config) throws Exception {
        databasePool = DatabaseService.createDatabaseConnectionPool("server");
        for (int i = 1000; i <= 1062; i++) {
            doWork(String.valueOf(i));
        }
//        for (int i = 1063; i <= 1260; i++) {
//            doWork(String.valueOf(i));
//        }
    }

    private void doWork(String id) throws SQLException, ParserConfigurationException, SAXException, IOException {
        System.out.println("-----");
        System.out.println("Start processing " + id);

        // Download
        String url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed21n" + id + ".xml.gz";
//        String url = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/pubmed21n" + id + ".xml.gz";
        BufferedInputStream inetIn = new BufferedInputStream(new URL(url).openStream());

        // Decompress
        GZIPInputStream gZIPInputStream = new GZIPInputStream(inetIn);

        // Parse the files
        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();
        PubmedSaxParser saxHandler = new PubmedSaxParser();
        saxParser.parse(gZIPInputStream, saxHandler);
        Deque<PubmedArticle> articles = saxHandler.getContext().getArticles();

        // Filter for COVID papers
        List<PubmedArticle> covidArticles = articles.stream().filter(a -> {
            if (Strings.isBlank(a.getArticleTitle()) || Strings.isBlank(a.getArticleAbstract())) {
                return false;
            }
            String merged = a.getArticleTitle().toLowerCase() + " " + a.getArticleAbstract().toLowerCase();
            // TODO this is super inefficient...
            return merged.contains("covid-19") || merged.contains("covid19") || merged.contains("sars-cov-2");
        }).collect(Collectors.toList());

        // Fetch the list of all existing articles
        Set<Long> pmidSet = new HashSet<>(fetchPmidOfExistingArticles());

        // Fetch the existing authors
        Map<PubmedAuthor, Long> authorToIdMap = new HashMap<>();
        for (PubmedAuthor fetchExistingAuthor : fetchExistingAuthors()) {
            authorToIdMap.put(fetchExistingAuthor, fetchExistingAuthor.getId());
        }

        System.out.println("Total articles: " + articles.size());
        System.out.println("COVID articles: " + covidArticles.size());

        // Write to the database
        writeToDatabase(covidArticles, pmidSet, authorToIdMap);

        // Close
        gZIPInputStream.close();
        inetIn.close();
    }

    private List<Long> fetchPmidOfExistingArticles() throws SQLException {
        List<Long> pmids = new ArrayList<>();
        String sql = """
            select pa.pmid
            from pubmed_article pa; 
        """;
        try (Connection conn = databasePool.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (rs.next()) {
                        pmids.add(rs.getLong("pmid"));
                    }
                }
            }
        }
        return pmids;
    }

    private List<PubmedAuthor> fetchExistingAuthors() throws SQLException {
        List<PubmedAuthor> authors = new ArrayList<>();
        String sql = """
            select
              pau.id,
              pau.lastname,
              pau.forename,
              pau.collective_name
            from pubmed_author pau;
        """;
        try (Connection conn = databasePool.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (rs.next()) {
                        authors.add(new PubmedAuthor()
                                .setId(rs.getLong("id"))
                                .setLastName(rs.getString("lastname"))
                                .setForeName(rs.getString("forename"))
                                .setCollectiveName(rs.getString("collective_name")));
                    }
                }
            }
        }
        return authors;
    }

    private void writeToDatabase(
            Iterable<PubmedArticle> articles,
            Set<Long> existingArticles,
            Map<PubmedAuthor, Long> existingAuthors
    ) throws SQLException {
        // We expect that the data files are imported chronologically and will update an entry if it already exists.
        Map<Long, PubmedArticle> articleInserts = new HashMap<>();
        List<Long> articleUpdates = new ArrayList<>();
        Set<PubmedAuthor> authorInserts = new HashSet<>();
        for (PubmedArticle article : articles) {
            articleInserts.put(article.getPmid(), article);
            if (!existingArticles.contains(article.getPmid())) {
                existingArticles.add(article.getPmid());
            } else {
                articleUpdates.add(article.getPmid());
            }
            for (PubmedAuthor author : article.getAuthors()) {
                author.setId(existingAuthors.get(author));
                if (author.getId() == null) {
                    authorInserts.add(author);
                }
            }
        }

        try (Connection conn = databasePool.getConnection()) {
            conn.setAutoCommit(false);
            // Insert the authors
            String insertAuthorSql = """
                insert into pubmed_author (lastname, forename, collective_name)\s
                values (?, ?, ?)
                returning id;
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertAuthorSql)) {
                for (PubmedAuthor author : authorInserts) {
                    statement.setString(1, author.getLastName());
                    statement.setString(2, author.getForeName());
                    statement.setString(3, author.getCollectiveName());
                    try (ResultSet rs = statement.executeQuery()) {
                        rs.next();
                        long id = rs.getLong(1);
                        author.setId(id);
                        existingAuthors.put(author, id);
                    }
                }
            }
            System.out.println("Inserted authors: " + articleInserts.size());

            // Delete articles that were updated
            String deleteArticleSql = """
                delete from pubmed_article
                where pmid = ?;
            """;
            try (PreparedStatement statement = conn.prepareStatement(deleteArticleSql)) {
                for (Long id : articleUpdates) {
                    statement.setLong(1, id);
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            // Insert the new articles
            String insertArticleSql = """
                insert into pubmed_article (pmid, date_completed, date_revised, article_title, journal_title, abstract, language)
                values (?, ?, ?, ?, ?, ?, ?);
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertArticleSql)) {
                for (PubmedArticle articleInsert : articleInserts.values()) {
                    statement.setLong(1, articleInsert.getPmid());
                    statement.setDate(2, articleInsert.getDateCompleted() != null ?
                            Date.valueOf(articleInsert.getDateCompleted()) : null);
                    statement.setDate(3, articleInsert.getDateRevised() != null ?
                            Date.valueOf(articleInsert.getDateRevised()) : null);
                    statement.setString(4, articleInsert.getArticleTitle());
                    statement.setString(5, articleInsert.getJournalTitle());
                    statement.setString(6, articleInsert.getArticleAbstract());
                    statement.setString(7, articleInsert.getLanguage());
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }
            System.out.println("Inserted new articles: " + (articleInserts.size() - articleUpdates.size()));
            System.out.println("Updated articles: " + articleUpdates.size());

            // Insert author-article mapping
            int i = 0;
            String insertArticleAuthorSql = """
                insert into pubmed_article__pubmed_author (pmid, author_id)
                values (?, ?);
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertArticleAuthorSql)) {
                for (PubmedArticle article : articleInserts.values()) {
                    Set<Long> authors = article.getAuthors().stream()
                            .map(existingAuthors::get)
                            .collect(Collectors.toSet());
                    for (Long author : authors) {
                        statement.setLong(1, article.getPmid());
                        statement.setLong(2, author);
                        statement.addBatch();
                        i++;
                    }
                }
                statement.executeBatch();
                statement.clearBatch();
            }
            System.out.println("Inserted article-author mappings: " + i);

            conn.commit();
            conn.setAutoCommit(true);
        }
    }
}
