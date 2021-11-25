package ch.ethz.harvester.playground;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.EmptyConfig;
import ch.ethz.harvester.core.SubProgram;
import org.javatuples.Triplet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class NexteraQC extends SubProgram<EmptyConfig> {
    public NexteraQC() {
        super("NexteraQC", EmptyConfig.class);
    }

    @Override
    public void run(String[] args, EmptyConfig config) throws Exception {
        List<Triplet<Integer, String, String>> samples = new ArrayList<>();
        try (Connection conn = DatabaseService.openDatabaseConnection("server")) {
            String sql = """
                select
                  cs1.ethid,
                  cs1.sample_name as original_sample_name,
                  cs2.sample_name as nextera_sample_name,
                  upper(cs1.seq) as original_seq,
                  upper(cs2.seq) as nextera_seq
                from
                  consensus_sequence cs1
                  join consensus_sequence cs2 on cs1.ethid = cs2.ethid and cs1.sample_name <> cs2.sample_name
                where
                  cs2.sample_name like '%_nextera';
            """;
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (rs.next()) {
                        samples.add(new Triplet<>(
                                rs.getInt("ethid"),
                                rs.getString("original_seq"),
                                rs.getString("nextera_seq")
                        ));
                    }
                }
            }
        }
        for (Triplet<Integer, String, String> sample : samples) {
            char[] original = sample.getValue1().toCharArray();
            char[] nextera = sample.getValue2().toCharArray();
            int baseDifferentlyCalled = 0;
            int baseSameCalled = 0;
            int baseToN = 0;
            int nToBase = 0;
            int bothN = 0;
            for (int i = 0; i < 29903; i++) {
                char o = original[i];
                char n = nextera[i];
                if (o != 'N' && n != 'N') {
                    if (o == n) {
                        baseSameCalled++;
                    } else {
                        baseDifferentlyCalled++;
                    }
                } else if (o != 'N') {
                    baseToN++;
                } else if (n != 'N') {
                    nToBase++;
                } else {
                    bothN++;
                }
            }
            System.out.println(sample.getValue0() + ","
                    + baseDifferentlyCalled + "," + baseSameCalled + "," + baseToN + "," + nToBase + "," + bothN);
        }
    }
}
