package ch.ethz.harvester.general;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.SubProgram;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class NucleotideMutationImporter extends SubProgram<NucleotideMutationImporterConfig> {

    private static class Sample {
        private final String sampleName;
        private final String sequence;

        public Sample(String sampleName, String sequence) {
            this.sampleName = sampleName;
            this.sequence = sequence;
        }
    }


    public NucleotideMutationImporter() {
        super("NucleotideMutationImporter", NucleotideMutationImporterConfig.class);
    }


    @Override
    public void run(String[] args, NucleotideMutationImporterConfig config) throws SQLException {
        try (Connection conn = DatabaseService.openDatabaseConnection(config.getVineyard())) {
            String reference = NucleotideMutationFinder.loadReferenceGenome(conn);
            Set<Integer> maskSites = NucleotideMutationFinder.loadMaskSites(conn);
            NucleotideMutationFinder mutationFinder = new NucleotideMutationFinder(reference, maskSites);
            while (true) {
                int inserted = importOurs(conn, mutationFinder);
                if (inserted == 0) {
                    break;
                }
            }
            while (true) {
                int inserted = importGisaidApi(conn, mutationFinder);
                if (inserted == 0) {
                    break;
                }
            }
        }
    }


    /**
     * @return The number of inserted mutations
     */
    private int importOurs(Connection conn, NucleotideMutationFinder mutationFinder) throws SQLException {
        // Load sequences
        List<Sample> samples = fetchOurSequences(conn);
        System.out.println(samples.size() + " sequences were loaded.");

        int numberMutations = 0;
        for (Sample sample : samples) {
            // Find mutations
            List<NucleotideMutationFinder.Mutation> mutations = mutationFinder.getMutations(sample.sequence);

            // Insert mutations to database
            insertForOurSequences(conn, sample.sampleName, mutations);
            numberMutations += mutations.size();
        }
        System.out.println(numberMutations + " mutations were inserted.");
        return numberMutations;
    }


    /**
     * @return The number of inserted mutations
     */
    private int importGisaidApi(Connection conn, NucleotideMutationFinder mutationFinder) throws SQLException {
        // Load sequences
        List<Sample> samples = fetchGisaidApiSequences(conn);
        System.out.println(samples.size() + " sequences were loaded.");

        int numberMutations = 0;
        for (Sample sample : samples) {
            // Find mutations
            List<NucleotideMutationFinder.Mutation> mutations = mutationFinder.getMutations(sample.sequence);

            // Insert mutations to database
            insertForGisaidApiSequences(conn, sample.sampleName, mutations);
            numberMutations += mutations.size();
        }
        System.out.println(numberMutations + " mutations were inserted.");
        return numberMutations;
    }


    private List<Sample> fetchOurSequences(Connection conn) throws SQLException {
        // Fetch sequences without assigned nucleotide mutations
        String fetchSeqsSql = """
            select
                cs.sample_name,
                upper(cs.seq) as seq
            from consensus_sequence cs
            where not exists(
                select *
                from consensus_sequence_mutation_nucleotide cs2
                where cs.sample_name = cs2.sample_name
            )
            limit 2000;
        """;
        List<Sample> samples = new ArrayList<>();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(fetchSeqsSql)) {
                while (rs.next()) {
                    samples.add(new Sample(rs.getString("sample_name"), rs.getString("seq")));
                }
            }
        }
        return samples;
    }


    private List<Sample> fetchGisaidApiSequences(Connection conn) throws SQLException {
        // Fetch sequences without assigned nucleotide mutations
        String fetchSeqsSql = """
            select
                gs.gisaid_epi_isl,
                upper(gs.seq_aligned) as seq
            from gisaid_api_sequence gs
            where
              not exists(
                select *
                from gisaid_api_sequence_mutation_nucleotide gs2
                where gs.gisaid_epi_isl = gs2.gisaid_epi_isl
              )
              and pangolin_lineage <> 'None'
            order by gs.gisaid_epi_isl
            limit 2000;
        """;
        List<Sample> samples = new ArrayList<>();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(fetchSeqsSql)) {
                while (rs.next()) {
                    samples.add(new Sample(rs.getString("gisaid_epi_isl"), rs.getString("seq")));
                }
            }
        }
        return samples;
    }


    private void insertForOurSequences(
            Connection conn,
            String sampleName,
            List<NucleotideMutationFinder.Mutation> mutations
    ) throws SQLException {
        insert(conn, sampleName, mutations, "consensus_sequence_mutation_nucleotide", "sample_name");
    }


    private void insertForGisaidApiSequences(
            Connection conn,
            String sampleName,
            List<NucleotideMutationFinder.Mutation> mutations
    ) throws SQLException {
        insert(conn, sampleName, mutations, "gisaid_api_sequence_mutation_nucleotide", "gisaid_epi_isl");
    }


    private void insert(
            Connection conn,
            String sampleName,
            List<NucleotideMutationFinder.Mutation> mutations,
            String tableName,
            String sampleNameColumn
    ) throws SQLException {
        conn.setAutoCommit(false);
        String insertSql = """
            insert into TABLE_NAME (SAMPLE_NAME_COLUMN, position, mutation)
            values (?, ?, ?);
        """.replace("TABLE_NAME", tableName).replace("SAMPLE_NAME_COLUMN", sampleNameColumn);
        try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
            for (NucleotideMutationFinder.Mutation mutation : mutations) {
                statement.setString(1, sampleName);
                statement.setInt(2, mutation.getPosition());
                statement.setString(3, String.valueOf(mutation.getMutation()));
                statement.addBatch();
            }
            statement.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
        }
    }
}
