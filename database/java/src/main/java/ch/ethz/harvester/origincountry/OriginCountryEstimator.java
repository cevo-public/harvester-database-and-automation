package ch.ethz.harvester.origincountry;

import ch.ethz.harvester.core.DatabaseService;
import ch.ethz.harvester.core.EmptyConfig;
import ch.ethz.harvester.core.SubProgram;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class OriginCountryEstimator extends SubProgram<EmptyConfig> {

    private static class Sample {
        private final String sampleName;
        private final String country;
        private final int[] mutationPositions;
        private final char[] mutationBases;
        private final boolean[] isUnknown;

        /**
         * @param country Country name
         * @param sequence An upper-case, aligned sequence
         * @param formattedMutationsList Mutations list, e.g., "123T,234C,345G"
         */
        public Sample(String sampleName, String country, char[] sequence, String formattedMutationsList) {
            this.sampleName = sampleName;
            this.country = country;
            isUnknown = new boolean[sequence.length];
            for (int i = 0; i < sequence.length; i++) {
                char base = sequence[i];
                if (base != 'C' && base != 'T' && base != 'A' && base != 'G') {
                    isUnknown[i] = true;
                }
            }
            String[] mutations = formattedMutationsList.split(",");
            mutationPositions = new int[mutations.length];
            mutationBases = new char[mutations.length];
            for (int i = 0; i < mutations.length; i++) {
                String mutation = mutations[i];
                mutationPositions[i] = Integer.parseInt(mutation.substring(0, mutation.length() - 1));
                mutationBases[i] = mutation.substring(mutation.length() - 1).charAt(0);
            }
        }
    }


    private static class SampleAndCountry {
        private final String sampleName;
        private final String country;

        public SampleAndCountry(
                String sampleName,
                String country
        ) {
            this.sampleName = sampleName;
            this.country = country;
        }
    }


    private final Random random = new Random();


    public OriginCountryEstimator() {
        super("OriginCountryEstimator", EmptyConfig.class);
    }


    @Override
    public void run(String[] args, EmptyConfig config) throws SQLException {
        if (args.length != 2) {
            System.out.println("Please provide a year and a month. Format: ..." + getName() + " <year> <month>");
            System.out.println("Example: ..." + getName() + " 2021 1");
            System.exit(1);
        }
        int year = Integer.parseInt(args[0]);
        int month = Integer.parseInt(args[1]);
        try (Connection conn = DatabaseService.openDatabaseConnection("server")) {
            doWork(conn, YearMonth.of(year, month));
        }
    }


    private void doWork(Connection conn, YearMonth yearMonth) throws SQLException {
        // Create sub sample sets
        System.out.println(LocalDateTime.now() + " Starting");
        List<List<Sample>> refSampleSets = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            refSampleSets.add(subsample(conn, yearMonth));
            System.out.println(LocalDateTime.now() + " Ref sample " + i + " loaded");
        }
        System.out.println(LocalDateTime.now() + " All ref samples loaded");

        // Fetch sequences without estimated countries
        int iteration = 0;
        while (true) {
            System.out.println(LocalDateTime.now() + " Processing batch " + iteration++);
            List<Sample> samples = fetchSamplesWithoutEstimatedCountries(conn, yearMonth);
            System.out.println(LocalDateTime.now() + " " + samples.size() + " samples loaded");
            if (samples.isEmpty()) {
                break;
            }
            List<SampleAndCountry> result = new ArrayList<>();
            for (Sample sample : samples) {
                for (List<Sample> refSampleSet : refSampleSets) {
                    result.add(new SampleAndCountry(
                            sample.sampleName,
                            getCountryOfClosestSample(sample, refSampleSet)
                    ));
                }
            }
            System.out.println(LocalDateTime.now() + " Estimated countries for " + samples.size() + " samples");
            insertResult(conn, result);
            System.out.println(LocalDateTime.now() + " Inserted " + samples.size());
        }
    }


    private List<Sample> subsample(Connection conn, YearMonth yearMonth) throws SQLException {
        String sql = """
            with cases_per_country as (
              select c.country, sum(c.cases) as n_cases
              from spectrum_sequence_intensity c
              where extract(year from c.date) = ? and extract(month from c.date) = ?
              group by c.country
            ),
            sequences as (
              select g.strain, g.country, g.aligned_seq
              from gisaid_sequence g
              where
                extract(year from g.date) = ? and extract(month from g.date) = ?
                and g.nextclade_total_missing < 1500
            ),
            samples_per_country as (
              select
                c.country,
                c.n_cases,
                ceil(c.n_cases * least(0.1 * (select count(*) from sequences), 10000.0)
                    / (select sum(n_cases) from cases_per_country)) as n_wanted_samples
              from cases_per_country c
            )
            select
              s.strain,
              s.country,
              string_agg(m.position || m.mutation, ',' order by m.position) as mutations,
              s.aligned_seq as seq
            from
              samples_per_country c
              join lateral (
                select *
                from sequences s
                where s.country = c.country
                order by random()
                limit c.n_wanted_samples
              ) s on true
              join gisaid_sequence_mutation_nucleotide m on s.strain = m.strain
            where c.n_cases is not null
            group by s.strain, s.country, s.aligned_seq
            order by s.country;
        """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, yearMonth.getYear());
            statement.setInt(2, yearMonth.getMonthValue());
            statement.setInt(3, yearMonth.getYear());
            statement.setInt(4, yearMonth.getMonthValue());
            try (ResultSet rs = statement.executeQuery()) {
                List<Sample> refSamples = new ArrayList<>();
                while (rs.next()) {
                    refSamples.add(new Sample(
                            null,
                            rs.getString("country"),
                            rs.getString("seq").toCharArray(),
                            rs.getString("mutations")
                    ));
                }
                return refSamples;
            }
        }
    }


    private List<Sample> fetchSamplesWithoutEstimatedCountries(
            Connection conn,
            YearMonth yearMonth
    ) throws SQLException {
        String sql = """
            select
              g.strain,
              string_agg(m.position || m.mutation, ',' order by m.position) as mutations,
              g.aligned_seq as seq
            from
              gisaid_sequence g
              join gisaid_sequence_mutation_nucleotide m on g.strain = m.strain
            where
              extract(year from g.date) = ? and extract(month from g.date) = ?
              and not exists(
                select
                from gisaid_sequence_close_country gscc
                where g.strain = gscc.strain
              )
            group by g.strain, g.aligned_seq
            limit 1000;
        """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, yearMonth.getYear());
            statement.setInt(2, yearMonth.getMonthValue());
            try (ResultSet rs = statement.executeQuery()) {
                List<Sample> samples = new ArrayList<>();
                while (rs.next()) {
                    samples.add(new Sample(
                            rs.getString("strain"),
                            null,
                            rs.getString("seq").toCharArray(),
                            rs.getString("mutations")
                    ));
                }
                return samples;
            }
        }
    }


    private String getCountryOfClosestSample(Sample sample, List<Sample> refSamples) {
        int lowestDistance = Integer.MAX_VALUE;
        String country = null;
        for (Sample refSample : refSamples) {
            int distance = 0;
            int i = 0;
            int j = 0;
            while (true) {
                if (sample.mutationBases.length <= i || refSample.mutationBases.length <= j) {
                    break;
                }
                // If sample does not have a mutation of refSample
                if (sample.mutationPositions[i] > refSample.mutationPositions[j]) {
                    if (!sample.isUnknown[refSample.mutationPositions[j]]) {
                        distance++;
                    }
                    j++;
                    continue;
                }
                // If refSample does not have a mutation of sample
                if (sample.mutationPositions[i] < refSample.mutationPositions[j]) {
                    if (!refSample.isUnknown[sample.mutationPositions[i]]) {
                        distance++;
                    }
                    i++;
                    continue;
                }
                // If both have a mutation at the same position but different mutations
                if (sample.mutationBases[i] != refSample.mutationBases[j]) {
                    distance++;
                }
                i++;
                j++;
            }
            if (distance < lowestDistance
                    || (distance == lowestDistance && random.nextBoolean())) {
                lowestDistance = distance;
                country = refSample.country;
            }
        }
        return country;
    }


    private void insertResult(Connection conn, List<SampleAndCountry> result) throws SQLException {
        conn.setAutoCommit(false);
        String insertSql = """
            insert into gisaid_sequence_close_country (
              strain,
              country_country
            ) values (?, ?);
        """;
        try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
            for (SampleAndCountry r : result) {
                statement.setString(1, r.sampleName);
                statement.setString(2, r.country);
                statement.addBatch();
            }
            statement.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
        }
    }
}
