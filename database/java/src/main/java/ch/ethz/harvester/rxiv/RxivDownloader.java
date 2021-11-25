package ch.ethz.harvester.rxiv;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.GlobalProxyManager;
import ch.ethz.harvester.core.Looper;
import ch.ethz.harvester.core.SubProgram;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.util.Strings;
import org.javatuples.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class RxivDownloader extends SubProgram<RxivDownloaderConfig> {
    public RxivDownloader() {
        super("RxivDownloader", RxivDownloaderConfig.class);
    }

    @Override
    public void run(String[] args, RxivDownloaderConfig config) throws Exception {
        GlobalProxyManager.setProxyFromConfig(config.getHttpProxy());
        Looper looper = new Looper(config.getLooper());
        while (looper.next()) {
            System.out.println(LocalDateTime.now() + " Running");
            importData(config, "biorxiv", LocalDate.now().minusDays(5), LocalDate.now());
            importData(config, "medrxiv", LocalDate.now().minusDays(5), LocalDate.now());
            System.out.println(LocalDateTime.now() + " Finished");
            looper.sleep();
        }
    }


    private void importData(
            RxivDownloaderConfig config,
            String server,
            LocalDate dateFrom,
            LocalDate dateTo
    ) throws SQLException, IOException, ParseException, InterruptedException {
        // Load IDs of existing articles and authors
        var existingArticleAndAuthors = loadExistingArticleAndAuthorIdAndInfo(config);
        Map<String, Integer> existingArticleVersions = existingArticleAndAuthors.getValue0();
        Map<String, Integer> existingAuthorIds = existingArticleAndAuthors.getValue1();

        // Find out how many pages we need to fetch
        int numberPages = determineNumberOfPages(server, dateFrom, dateTo);

        // Download from API
        List<RxivArticle> articles = new ArrayList<>();
        String url = getUrlPrefixWithoutPage(server, dateFrom, dateTo);
        for (int i = 0; i < numberPages; i++) {
            int offset = i * 100;
            InputStream in = new URL(url + offset).openStream();
            String jsonString = IOUtils.toString(in, StandardCharsets.UTF_8);
            JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
            JSONArray collection = (JSONArray) json.get("collection");
            for (Object o : collection) {
                JSONObject entry = (JSONObject) o;
                List<String> authors = Arrays.stream(((String) entry.get("authors")).split(";"))
                        .map(String::trim)
                        .collect(Collectors.toList());
                RxivArticle article = new RxivArticle()
                        .setDoi((String) entry.get("doi"))
                        .setTitle((String) entry.get("title"))
                        .setAuthors(authors)
                        .setDate(LocalDate.parse((String) entry.get("date")))
                        .setVersion(Integer.parseInt((String) entry.get("version")))
                        .setType((String) entry.get("type"))
                        .setLicense((String) entry.get("license"))
                        .setCategory((String) entry.get("category"))
                        .setJatsxmlUrl((String) entry.get("jatsxml"))
                        .setAbstractText((String) entry.get("abstract"))
                        .setPublished(entry.get("published").equals("NA") ? null : (String) entry.get("published"))
                        .setServer((String) entry.get("server"));
                articles.add(article);
            }
            // To be polite and not spam the API ;)
            Thread.sleep(600);
        }

        // Filter for COVID papers
        List<RxivArticle> covidArticles = articles.stream().filter(a -> {
            if (Strings.isBlank(a.getTitle()) || Strings.isBlank(a.getAbstractText())) {
                return false;
            }
            String merged = a.getTitle().toLowerCase() + " " + a.getAbstractText().toLowerCase();
            // TODO this is super inefficient...
            return merged.contains("covid-19") || merged.contains("covid19") || merged.contains("sars-cov-2");
        }).collect(Collectors.toList());

        // Identify articles that need to be updated
        Map<String, RxivArticle> articleInserts = new HashMap<>();
        Set<String> articleDeletes = new HashSet<>(); // Instead of an actual update, we will delete and re-insert.
        for (RxivArticle article : covidArticles) {
            String doi = article.getDoi();
            if (existingArticleVersions.containsKey(doi)) {
                if (existingArticleVersions.get(doi) >= article.getVersion()) {
                    // The same or a newer version is already in the database or was found in the same dataset
                    continue;
                }
                articleDeletes.add(article.getDoi());
            }
            articleInserts.put(doi, article);
        }

        System.out.println("Number pages: " + numberPages);
        System.out.println("Total articles: " + articles.size());
        System.out.println("COIVD articles: " + covidArticles.size());
        System.out.println("New articles: " + (articleInserts.size() - articleDeletes.size()));
        System.out.println("Updated articles: " + articleDeletes.size());

        // Write to database
        writeToDatabase(config, articleInserts, articleDeletes, existingAuthorIds);
    }

    private Pair<Map<String, Integer>, Map<String, Integer>> loadExistingArticleAndAuthorIdAndInfo(
            RxivDownloaderConfig config
    ) throws SQLException {
        Map<String, Integer> articleVersions = new HashMap<>();
        Map<String, Integer> authorIds = new HashMap<>();
        try (Connection conn = DatabaseService.openDatabaseConnection(config.getVineyard())) {
            String loadArticles = """
                select doi, version
                from rxiv_article;
            """;
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(loadArticles)) {
                    while (rs.next()) {
                        articleVersions.put(
                                rs.getString("doi"),
                                rs.getInt("version")
                        );
                    }
                }
            }
            String loadAuthors = """
                 select name, id
                 from rxiv_author;
            """;
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(loadAuthors)) {
                    while (rs.next()) {
                        authorIds.put(
                                rs.getString("name"),
                                rs.getInt("id")
                        );
                    }
                }
            }
        }
        return new Pair<>(articleVersions, authorIds);
    }


    private int determineNumberOfPages(
            String server,
            LocalDate dateFrom,
            LocalDate dateTo
    ) throws IOException, ParseException {
        String url = getUrlPrefixWithoutPage(server, dateFrom, dateTo) + 0;
        InputStream in = new URL(url).openStream();
        String jsonString = IOUtils.toString(in, StandardCharsets.UTF_8);
        JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
        JSONArray messages = (JSONArray) json.get("messages");
        if (messages.size() != 1) {
            System.out.println("Unexpected number of messages. " + url);
        }
        JSONObject firstMessage = (JSONObject) messages.get(0);
        if (!"ok".equals(firstMessage.get("status"))) {
            System.out.println("The API reported a non-OK status. " + url);
        }
        long totalPapers = (long) firstMessage.get("total");
        return (int) Math.ceil(totalPapers / 100.0);
    }


    private String getUrlPrefixWithoutPage(String server, LocalDate dateFrom, LocalDate dateTo) {
        return "https://api.biorxiv.org/details/" + server + "/" + dateFrom.toString() + "/" + dateTo.toString() + "/";
    }


    private void writeToDatabase(
            RxivDownloaderConfig config,
            Map<String, RxivArticle> articleInserts,
            Set<String> articleDeletes,
            Map<String, Integer> existingAuthorIds
    ) throws SQLException {
        Set<String> authorInserts = articleInserts.values().stream()
                .flatMap(a -> a.getAuthors().stream())
                .filter(a -> !existingAuthorIds.containsKey(a))
                .collect(Collectors.toSet());

        try (Connection conn = DatabaseService.openDatabaseConnection(config.getVineyard())) {
            conn.setAutoCommit(false);

            String insertAuthorSql = """
                insert into rxiv_author (name)
                values (?)
                returning id;
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertAuthorSql)) {
                for (String author : authorInserts) {
                    statement.setString(1, author);
                    try (ResultSet rs = statement.executeQuery()) {
                        rs.next();
                        existingAuthorIds.put(author, rs.getInt(1));
                    }
                }
            }

            String deleteArticleSql = """
                delete from rxiv_article
                where doi = ?;
            """;
            try (PreparedStatement statement = conn.prepareStatement(deleteArticleSql)) {
                for (String doi : articleDeletes) {
                    statement.setString(1, doi);
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            String insertArticleSql = """
                insert into rxiv_article (
                  doi, version, title, date, type, category, abstract, license, server,
                  jatsxml, published)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertArticleSql)) {
                for (RxivArticle artice : articleInserts.values()) {
                    statement.setString(1, artice.getDoi());
                    statement.setInt(2, artice.getVersion());
                    statement.setString(3, artice.getTitle());
                    statement.setDate(4, Date.valueOf(artice.getDate()));
                    statement.setString(5, artice.getType());
                    statement.setString(6, artice.getCategory());
                    statement.setString(7, artice.getAbstractText());
                    statement.setString(8, artice.getLicense());
                    statement.setString(9, artice.getServer());
                    statement.setString(10, artice.getJatsxmlUrl());
                    statement.setString(11, artice.getPublished());
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            String insertArticleAuthorSql = """
                insert into rxiv_article__rxiv_author (doi, author_id, position)
                values (?, ?, ?);
            """;
            try (PreparedStatement statement = conn.prepareStatement(insertArticleAuthorSql)) {
                for (RxivArticle article : articleInserts.values()) {
                    int position = 1;
                    for (String author : article.getAuthors()) {
                        statement.setString(1, article.getDoi());
                        statement.setInt(2, existingAuthorIds.get(author));
                        statement.setInt(3, position++);
                        statement.addBatch();
                    }
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            conn.commit();
            conn.setAutoCommit(true);
        }
    }
}
