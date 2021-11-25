package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.EmptyConfig;
import ch.ethz.harvester.core.SubProgram;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;


public class SubmitterInformationFetcher extends SubProgram<EmptyConfig> {

    public SubmitterInformationFetcher() {
        super("SubmitterInformationFetcher", EmptyConfig.class);
    }

    public Optional<SubmitterInformation> fetchSubmitterInformation(String gisaidEpiIsl) {
        try {
            String accessionNumber = gisaidEpiIsl.split("_")[2];
            int l = accessionNumber.length();
            String url = "https://www.epicov.org/acknowledgement/" + accessionNumber.substring(l - 4, l - 2)
                    + "/" + accessionNumber.substring(l - 2, l)
                    + "/" + gisaidEpiIsl + ".json";
            String jsonString = IOUtils.toString(new URL(url).openStream(), StandardCharsets.UTF_8);
            JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
            String jsonO = (String) json.get("covv_orig_lab");
            String jsonS = (String) json.get("covv_subm_lab");
            String jsonA = (String) json.get("covv_authors");
            SubmitterInformation result = new SubmitterInformation()
                    .setOriginatingLab("na".equalsIgnoreCase(jsonO) ? null : jsonO)
                    .setSubmittingLab("na".equalsIgnoreCase(jsonS) ? null : jsonS)
                    .setAuthors("na".equalsIgnoreCase(jsonA) ? null : jsonA);
            return Optional.of(result);
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    @Override
    public void run(String[] args, EmptyConfig config) throws Exception {
        try (Connection conn = DatabaseService.openDatabaseConnection("server")) {
            while (true) {
                // Fetch the IDs of the sequences where the submitter information is unknown
                String fetchIdsSql = """
                    select gisaid_epi_isl
                    from gisaid_api_sequence
                    where
                      submitting_lab is null
                      and originating_lab is null
                      and authors is null
                    limit 1000;
                """;
                List<String> ids = new ArrayList<>();
                try (Statement statement = conn.createStatement()) {
                    try (ResultSet rs = statement.executeQuery(fetchIdsSql)) {
                        while (rs.next()) {
                            ids.add(rs.getString("gisaid_epi_isl"));
                        }
                    }
                }

                // Fetch the submitter information
                Map<String, SubmitterInformation> submitterInformationMap = new HashMap<>();
                for (String id : ids) {
                    Optional<SubmitterInformation> submitterOpt = fetchSubmitterInformation(id);
                    submitterOpt.ifPresent(submitterInformation -> submitterInformationMap.put(id, submitterInformation));
                }

                // Write the submitter information to the database
                String insertSql = """
                    update gisaid_api_sequence
                    set
                      originating_lab = ?,
                      submitting_lab = ?,
                      authors = ?
                    where gisaid_epi_isl = ?;
                """;
                conn.setAutoCommit(false);
                try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
                    for (Map.Entry<String, SubmitterInformation> entry : submitterInformationMap.entrySet()) {
                        String gisaidId = entry.getKey();
                        SubmitterInformation submitterInformation = entry.getValue();
                        statement.setString(1, submitterInformation.getOriginatingLab());
                        statement.setString(2, submitterInformation.getSubmittingLab());
                        statement.setString(3, submitterInformation.getAuthors());
                        statement.setString(4, gisaidId);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                }
                conn.commit();
                conn.setAutoCommit(true);

                int inserted = submitterInformationMap.size();
                System.out.println("Inserted submitter information for " + inserted + " sequences");
                if (inserted == 0) {
                    break;
                }
            }
        }
    }
}
