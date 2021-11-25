package ch.ethz.harvester.pangolineage;

import ch.ethz.harvester.core.*;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PangolinLineageAliasImporter extends SubProgram<PangolinLineageAliasImporterConfig> {
    private final static String DATA_URL
            = "https://raw.githubusercontent.com/cov-lineages/pango-designation/master/pango_designation/alias_key.json";

    public PangolinLineageAliasImporter() {
        super("PangolinLineageAliasImporter", PangolinLineageAliasImporterConfig.class);
    }

    @Override
    public void run(String[] args, PangolinLineageAliasImporterConfig config) throws Exception {
        NotificationSystem notificationSystem = new NotificationSystemFactory()
                .createNotificationSystemFromConfig(config.getNotification());
        try {
            GlobalProxyManager.setProxyFromConfig(config.getHttpProxy());
            Looper looper = new Looper(config.getLooper());
            while (looper.next()) {
                doWork(notificationSystem, config);
                looper.sleep();
            }
        } catch (Throwable e) {
            notificationSystem.sendReport(new ProgramCrashReport(e, "PangolinLineageAliasImporter"));
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void doWork(NotificationSystem notificationSystem, PangolinLineageAliasImporterConfig config)
            throws URISyntaxException, IOException, ParseException, SQLException {
        // Fetch data from the official repository and parse the JSON
        List<PangolinLineageAlias> remoteAliases = new ArrayList<>();
        String jsonStr = IOUtils.toString(new URI(DATA_URL), StandardCharsets.UTF_8);
        JSONObject json = (JSONObject) new JSONParser().parse(jsonStr);
        for (Object keyObj : json.keySet()) {
            String key = (String) keyObj;
            Object valueObj = json.get(key);
            if (valueObj instanceof JSONArray) {
                // This is a recombinant. For now, we ignore them.
                continue;
            }
            if (!(valueObj instanceof String)) {
                throw new RuntimeException("Unexpected JSON file format: " + DATA_URL);
            }
            String value = (String) valueObj;
            // We don't check the validity of the value, yet. That will be done in the change detection step.
            remoteAliases.add(new PangolinLineageAlias(key, value));
        }

        try (Connection conn = DatabaseService.openDatabaseConnection(config.getVineyard())) {
            // Load current data from the database
            List<PangolinLineageAlias> existingAliases = new ArrayList<>();
            String fetchSql = """
                select alias, full_name
                from pangolin_lineage_alias;
            """;
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(fetchSql)) {
                    while (rs.next()) {
                        existingAliases.add(new PangolinLineageAlias(
                                rs.getString("alias"),
                                rs.getString("full_name")
                        ));
                    }
                }
            }

            // Perform validity and change detection: which were added, deleted, and changed
            Map<String, String> existingAliasesMap = new HashMap<>();
            for (PangolinLineageAlias existingAlias : existingAliases) {
                existingAliasesMap.put(existingAlias.getAlias(), existingAlias.getFullName());
            }
            Set<String> remoteAliasesKeySet = new HashSet<>();
            List<PangolinLineageAlias> toAdd = new ArrayList<>();
            List<PangolinLineageAlias> toDelete = new ArrayList<>();
            List<PangolinLineageAlias> toUpdate = new ArrayList<>();
            List<PangolinLineageAlias> unexpected = new ArrayList<>();
            for (PangolinLineageAlias remoteAlias : remoteAliases) {
                String alias = remoteAlias.getAlias();
                String fullName = remoteAlias.getFullName();
                remoteAliasesKeySet.add(alias);
                if (fullName.isBlank() && !alias.equals("A") && !alias.equals("B")) {
                    // Unexpected root lineage
                    unexpected.add(remoteAlias);
                }
                if (fullName.isBlank()) {
                    continue;
                }
                if (!isExpectedFullName(remoteAlias.getFullName())) {
                    // Unexpected full name
                    unexpected.add(remoteAlias);
                }
                String existingFullName = existingAliasesMap.get(alias);
                if (existingFullName == null) {
                    toAdd.add(remoteAlias);
                } else if (!existingFullName.equals(fullName)) {
                    toUpdate.add(remoteAlias);
                }
            }
            for (PangolinLineageAlias existingAlias : existingAliases) {
                if (!remoteAliasesKeySet.contains(existingAlias.getAlias())) {
                    toDelete.add(existingAlias);
                }
            }

            // Leave if nothing interesting happened
            if (unexpected.isEmpty() && toAdd.isEmpty() && toUpdate.isEmpty() && toDelete.isEmpty()) {
                System.out.println(LocalDateTime.now() + " Nothing to do.");
                return;
            }

            // Send an email with the changes and unexpected entries
            ImportReport report = new ImportReport(toAdd, toDelete, toUpdate, unexpected);
            notificationSystem.sendReport(report);

            // Leave if there are unexpected entries
            if (!unexpected.isEmpty()) {
                System.out.println(LocalDateTime.now() + " Unexpected entries!");
                return;
            }

            // Perform changes
            System.out.println(LocalDateTime.now() + " Start writing to the database.");
            conn.setAutoCommit(false);
            String deleteSql = """
                delete from pangolin_lineage_alias where alias = ?;
            """;
            if (!toDelete.isEmpty()) {
                try (PreparedStatement statement = conn.prepareStatement(deleteSql)) {
                    for (PangolinLineageAlias pangolinLineageAlias : toDelete) {
                        statement.setString(1, pangolinLineageAlias.getAlias());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            String updateSql = """
                update pangolin_lineage_alias
                set full_name = ?
                where alias = ?;
            """;
            if (!toUpdate.isEmpty()) {
                try (PreparedStatement statement = conn.prepareStatement(updateSql)) {
                    for (PangolinLineageAlias pangolinLineageAlias : toUpdate) {
                        statement.setString(1, pangolinLineageAlias.getFullName());
                        statement.setString(2, pangolinLineageAlias.getAlias());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            String insertSql = """
                insert into pangolin_lineage_alias (alias, full_name)
                values (?, ?);
            """;

            if (!toAdd.isEmpty()) {
                try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
                    for (PangolinLineageAlias pangolinLineageAlias : toAdd) {
                        statement.setString(1, pangolinLineageAlias.getAlias());
                        statement.setString(2, pangolinLineageAlias.getFullName());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    private boolean isExpectedFullName(String fullName) {
        Pattern pattern = Pattern.compile("[A-Z]{1,2}(\\.[0-9]{1,3}){3}");
        Matcher matcher = pattern.matcher(fullName);
        return matcher.matches();
    }
}
