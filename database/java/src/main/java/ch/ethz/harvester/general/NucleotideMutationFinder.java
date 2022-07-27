package ch.ethz.harvester.general;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class NucleotideMutationFinder {
    public static class Mutation {
        private final int position;
        private final char mutation;

        public Mutation(int position, char mutation) {
            this.position = position;
            this.mutation = mutation;
        }

        public int getPosition() {
            return position;
        }

        public char getMutation() {
            return mutation;
        }
    }

    public static String loadReferenceGenome(Connection conn) throws SQLException {
        String sql = """
            select upper(cs.seq) as seq
            from backup_220530_consensus_sequence cs
            where sample_name = 'REFERENCE_GENOME';
        """;
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                rs.next();
                return rs.getString("seq");
            }
        }
    }

    public static Set<Integer> loadMaskSites(Connection conn) throws SQLException {
        String sql = """
            select position
            from ext_problematic_site
            where filter = 'mask';
        """;
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                Set<Integer> sites = new HashSet<>();
                while (rs.next()) {
                    sites.add(rs.getInt("position"));
                }
                return sites;
            }
        }
    }

    private final char[] reference;

    private  final Set<Integer> maskSites;

    /**
     * @param reference The reference genome
     * @param maskSites The positions of the sites that should be masked. The positions are 1-indexed.
     */
    public NucleotideMutationFinder(String reference, Set<Integer> maskSites) {
        this.reference = reference.toUpperCase().toCharArray();
        this.maskSites = maskSites;
    }

    public List<Mutation> getMutations(String sequence) {
        return getMutations(sequence.toUpperCase().toCharArray());
    }

    private List<Mutation> getMutations(char[] sequence) {
        if (sequence.length != reference.length) {
            throw new RuntimeException("The sequence does not have the same length as the reference. " +
                    "Please align the sequence first.");
        }

        // Masking leading and tailing deletions because they are often actually unknowns but appear here as
        // deletions due to aligning.
        for (int i = 0; i < sequence.length; i++) {
            if (sequence[i] != '-') {
                break;
            }
            sequence[i] = 'N';
        }
        for (int i = sequence.length - 1; i >= 0; i--) {
            if (sequence[i] != '-') {
                break;
            }
            sequence[i] = 'N';
        }

        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < reference.length; i++) {
            int pos = i + 1;
            char refBase = reference[i];
            char seqBase = sequence[i];
            if (maskSites.contains(pos)) {
                continue;
            }
            if (seqBase != 'C' && seqBase != 'T' && seqBase != 'A' && seqBase != 'G' && seqBase != '-') {
                continue;
            }
            if (seqBase != refBase) {
                mutations.add(new Mutation(pos, seqBase));
            }
        }
        return mutations;
    }
}
