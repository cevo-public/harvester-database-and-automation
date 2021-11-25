package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.core.Utils;
import ch.ethz.harvester.general.NucleotideMutationFinder;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.javatuples.Pair;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BatchProcessingWorker {

    // Code based on https://www.baeldung.com/run-shell-command-in-java
    // and https://github.com/eugenp/tutorials/tree/master/core-java-modules/core-java-os (MIT license)
    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }


    private final int id;
    private final Path workDir;
    private final Path referenceFasta;
    private final ComboPooledDataSource databasePool;
    private final ImportMode importMode;
    private final boolean updateSubmitterInformation;
    private final NucleotideMutationFinder nucleotideMutationFinder;
    private final EthzParser ethzParser;
    private final SubmitterInformationFetcher submitterInformationFetcher = new SubmitterInformationFetcher();

    /**
     *
     * @param id An unique identifier for the worker
     * @param workDir An empty work directory for the worker
     * @param referenceFasta The path to the fasta file containing the reference
     */
    public BatchProcessingWorker(
            int id,
            Path workDir,
            Path referenceFasta,
            ComboPooledDataSource databasePool,
            ImportMode importMode,
            boolean updateSubmitterInformation,
            NucleotideMutationFinder nucleotideMutationFinder
    ) {
        this.databasePool = databasePool;
        this.id = id;
        this.workDir = workDir;
        this.referenceFasta = referenceFasta;
        this.importMode = importMode;
        this.updateSubmitterInformation = updateSubmitterInformation;
        this.nucleotideMutationFinder = nucleotideMutationFinder;
        this.ethzParser = new EthzParser();
    }

    public BatchReport run(Batch batch) throws Exception {
        try {
            int batchSize = batch.getSequences().size();
            System.out.println("[" + id + "] Received a batch");
            List<WeirdEntryReport> weirdEntryReports = new ArrayList<>();

            // Remove entries from the batch where no sequence is provided -> very weird
            batch = new Batch(batch.getSequences().stream().filter(s -> {
                if (s.getSeqOriginal() == null || s.getSeqOriginal().isBlank()) {
                    weirdEntryReports.add(
                            new WeirdEntryReport(s.getGisaidEpiIsl(),
                                    "ch.ethz.harvester.gisaid.BatchReport::run",
                                    "No sequence was provided."));
                    return false;
                }
                return true;
            }).collect(Collectors.toList()));

            if (importMode == ImportMode.APPEND) {
                for (Sequence sequence : batch.getSequences()) {
                    sequence.setImportMode(ImportMode.APPEND);
                }
            } else if (importMode == ImportMode.UPDATE) {
                determineChangeSet(batch);
            }

            // Fetch the submitter information for all APPEND sequences
            for (Sequence sequence : batch.getSequences()) {
                if (sequence.getImportMode() == ImportMode.APPEND) {
                    submitterInformationFetcher.fetchSubmitterInformation(sequence.getGisaidEpiIsl())
                            .ifPresent(sequence::setSubmitterInformation);
                }
            }

            // Determine the sequences that need to be processed by mafft and Nextclade
            List<Sequence> sequencePreprocessingNeeded = batch.getSequences().stream()
                    .filter(s -> s.getImportMode() != ImportMode.UPDATE || s.isSequenceChanged())
                    .collect(Collectors.toList());

            System.out.println("[" + id + "] " + sequencePreprocessingNeeded.size() + " out of " + batchSize + " sequences are new or have changed sequence.");
            if (!sequencePreprocessingNeeded.isEmpty()) {
                // Write the batch to a fasta file
                Path originalSeqFastaPath = workDir.resolve("original.fasta");
                System.out.println("[" + id + "] Write fasta to disk..");
                Files.writeString(originalSeqFastaPath, formatSeqAsFasta(sequencePreprocessingNeeded));

                // Run mafft to align the sequences with our reference sequence
                System.out.println("[" + id + "] Run mafft..");
                // TODO For unknown reasons, mafft is sometimes failing. For now, we will allow a few failed batches.
                try {
                    runMafft(batch, originalSeqFastaPath);
                } catch (RuntimeException e) {
                    System.out.println("[" + id + "] mafft is struggling: " + e.getMessage());
                    return new BatchReport().setFailedEntries(batch.getSequences().size());
                }

                // Run Nextclade for the amino acid mutations and the QC metrics
                System.out.println("[" + id + "] Run Nextclade..");
                runNextclade(batch, originalSeqFastaPath);

                // Extract the nucleotide mutations
                for (Sequence sequence : batch.getSequences()) {
                    if (sequence.getPangolinLineage() != null && !sequence.getPangolinLineage().equals("None")
                            && sequence.getSeqAligned() != null) {
                        List<NucleotideMutationFinder.Mutation> nucMutations
                                = nucleotideMutationFinder.getMutations(sequence.getSeqAligned());
                        sequence.setNucleotideMutations(nucMutations);
                    }
                }
            }

            // Write the data into the database
            System.out.println("[" + id + "] Write to database..");
            writeToDatabase(batch);

            // If the sequence is submitted by us, update the sequence_identifier table.
            Pair<List<WeirdEntryReport>, Integer> updateSequenceIdentifierReport = updateSequenceIdentifier(batch);
            weirdEntryReports.addAll(updateSequenceIdentifierReport.getValue0());
            EthzParser ethzParser = new EthzParser();
            int addedEntriesFromUs = 0;
            for (Sequence sequence : batch.getSequences()) {
                if (sequence.getImportMode() == ImportMode.APPEND) {
                    MaybeResult<Boolean> isOursMaybe = ethzParser.isOurs(sequence);
                    if (isOursMaybe.isGoodEnough() && isOursMaybe.getResult()) {
                        addedEntriesFromUs++;
                    }
                }
            }

            // Create the batch report
            int addedEntries = 0;
            int updatedTotalEntries = 0;
            int updatedMetadataEntries = 0;
            int updatedSequenceEntries = 0;
            for (Sequence sequence : batch.getSequences()) {
                if (sequence.getImportMode() == ImportMode.APPEND) {
                    addedEntries++;
                } else if (sequence.getImportMode() == ImportMode.UPDATE) {
                    updatedTotalEntries++;
                    if (sequence.isMetadataChanged()) {
                        updatedMetadataEntries++;
                    }
                    if (sequence.isSequenceChanged()) {
                        updatedSequenceEntries++;
                    }
                }
            }
            System.out.println("[" + id + "] Everything successful with no failed sequences");
            return new BatchReport()
                    .setAddedEntries(addedEntries)
                    .setUpdatedTotalEntries(updatedTotalEntries)
                    .setUpdatedMetadataEntries(updatedMetadataEntries)
                    .setUpdatedSequenceEntries(updatedSequenceEntries)
                    .setAddedEntriesFromUs(addedEntriesFromUs)
                    .setWeirdEntryReports(weirdEntryReports);
        } finally {
            // Clean up the work directory
            try (DirectoryStream<Path> directory = Files.newDirectoryStream(workDir)) {
                for (Path path : directory) {
                    Files.delete(path);
                }
            }
            System.out.println("[" + id + "] Done!");
        }
    }


    /**
     * Fetch the data of the sequences from the database and compare them with the downloaded data.
     * If an entry is already in the database and has not changed, remove it from the batch. If an entry is already
     * in the database and has changed, mark the entry as an update candidate. The method also checks whether the
     * metadata or the sequence was changed.
     */
    private void determineChangeSet(Batch batch) throws SQLException {
        Map<String, Sequence> sequenceMap = new HashMap<>();
        for (Sequence sequence : batch.getSequences()) {
            sequence.setImportMode(ImportMode.APPEND);
            sequenceMap.put(sequence.getGisaidEpiIsl(), sequence);
        }
        String fetchSql = """
            select
              gisaid_epi_isl,
              strain,
              virus,
              date,
              date_original,
              country,
              region_original,
              country_original,
              division,
              location,
              host,
              age,
              sex,
              pangolin_lineage,
              gisaid_clade,
              originating_lab,
              submitting_lab,
              authors,
              date_submitted,
              sampling_strategy,
              seq_original
            from gisaid_api_sequence
            where gisaid_epi_isl = any(?);
        """;
        try (Connection conn = databasePool.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(fetchSql)) {
                Object[] gisaidIds = batch.getSequences().stream().map(Sequence::getGisaidEpiIsl).toArray();
                statement.setArray(1, conn.createArrayOf("text", gisaidIds));
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        Sequence sequence = sequenceMap.get(rs.getString("gisaid_epi_isl"));
                        sequence.setImportMode(ImportMode.UPDATE);
                        // If updateSubmitterInformation is true, we need to fetch these data.
                        if (updateSubmitterInformation) {
                            Optional<SubmitterInformation> submitterInformationOpt
                                    = submitterInformationFetcher.fetchSubmitterInformation(sequence.getGisaidEpiIsl());
                            submitterInformationOpt.ifPresent(sequence::setSubmitterInformation);
                        }
                        sequence.setMetadataChanged(true);
                        sequence.setSequenceChanged(true);
                        if (Objects.equals(sequence.getStrain(), rs.getString("strain"))
                                && Objects.equals(sequence.getVirus(), rs.getString("virus"))
                                && Objects.equals(sequence.getDate(), rs.getDate("date") != null ? rs.getDate("date").toLocalDate() : null)
                                && Objects.equals(sequence.getDateOriginal(), rs.getString("date_original"))
                                && Objects.equals(sequence.getCountry(), rs.getString("country"))
                                && Objects.equals(sequence.getRegionOriginal(), rs.getString("region_original"))
                                && Objects.equals(sequence.getCountryOriginal(), rs.getString("country_original"))
                                && Objects.equals(sequence.getDivision(), rs.getString("division"))
                                && Objects.equals(sequence.getLocation(), rs.getString("location"))
                                && Objects.equals(sequence.getHost(), rs.getString("host"))
                                && Objects.equals(sequence.getAge(), rs.getObject("age"))
                                && Objects.equals(sequence.getSex(), rs.getString("sex"))
                                && Objects.equals(sequence.getPangolinLineage(), rs.getString("pangolin_lineage"))
                                && Objects.equals(sequence.getGisaidClade(), rs.getString("gisaid_clade"))
                                && Objects.equals(sequence.getDateSubmitted(), rs.getDate("date_submitted") != null ? rs.getDate("date_submitted").toLocalDate() : null)
                                && Objects.equals(sequence.getSamplingStrategy(), rs.getString("sampling_strategy"))
                                // Compare submitter information if it has been fetched
                                && (sequence.getSubmitterInformation() == null || (
                                        Objects.equals(sequence.getSubmitterInformation().getOriginatingLab(), rs.getString("originating_lab"))
                                                && Objects.equals(sequence.getSubmitterInformation().getSubmittingLab(), rs.getString("submitting_lab"))
                                                && Objects.equals(sequence.getSubmitterInformation().getAuthors(), rs.getString("authors"))
                                ))
                        ) {
                            sequence.setMetadataChanged(false);
                        }
                        if (Objects.equals(sequence.getSeqOriginal(), rs.getString("seq_original"))) {
                            sequence.setSequenceChanged(false);
                        }
                        if (!sequence.isMetadataChanged() && !sequence.isSequenceChanged()) {
                            batch.getSequences().remove(sequence);
                        }
                    }
                }
            }
        }
    }


    private String formatSeqAsFasta(List<Sequence> sequences) {
        StringBuilder fasta = new StringBuilder("");
        for (Sequence sequence : sequences) {
            fasta
                    .append(">")
                    .append(sequence.getGisaidEpiIsl())
                    .append("\n")
                    .append(sequence.getSeqOriginal())
                    .append("\n\n");
        }
        return fasta.toString();
    }


    private Map<String, String> parseFasta(List<String> lines) {
        Map<String, String> sequences = new HashMap<>();
        String name = null;
        StringBuilder seq = null;
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            if (line.startsWith(">")) {
                if (name != null) {
                    sequences.put(name, seq.toString());
                }
                name = line.substring(1);
                seq = new StringBuilder();
            } else {
                seq.append(line);
            }
        }
        if (name != null) {
            sequences.put(name, seq.toString());
        }
        return sequences;
    }


    private void runMafft(Batch batch, Path originalSeqFastaPath) throws IOException, InterruptedException {
        String mafftCommand = "mafft" +
                " --addfragments " + originalSeqFastaPath.toAbsolutePath() +
                " --keeplength" +
                " --auto" +
                " --thread 1" +
                " " + referenceFasta.toAbsolutePath();
        Process mafftProcess = Runtime.getRuntime().exec(mafftCommand);
        List<String> mafftInputLines = new ArrayList<>();
        StreamGobbler mafftGobbler = new StreamGobbler(mafftProcess.getInputStream(), mafftInputLines::add);
        ExecutorService inputStreamReaderService = Executors.newSingleThreadExecutor();
        inputStreamReaderService.submit(mafftGobbler);
        boolean exited = mafftProcess.waitFor(20, TimeUnit.MINUTES);
        if (!exited) {
            mafftProcess.destroyForcibly();
            throw new RuntimeException("mafft timed out (after 20 minutes)");
        }
        if (mafftProcess.exitValue() != 0) {
            throw new RuntimeException("mafft exited with code " + mafftProcess.exitValue());
        }
        inputStreamReaderService.shutdown();
        boolean inputStreamReaderTerminated = inputStreamReaderService.awaitTermination(5, TimeUnit.SECONDS);
        if (!inputStreamReaderTerminated) {
            throw new RuntimeException("The input stream reader did not terminate after executing mafft!");
        }
        Map<String, Sequence> sequenceMap = new HashMap<>();
        for (Sequence sequence : batch.getSequences()) {
            sequenceMap.put(sequence.getGisaidEpiIsl(), sequence);
        }
        Map<String, String> alignedSeqsMap = parseFasta(mafftInputLines);
        for (Map.Entry<String, String> entry : alignedSeqsMap.entrySet()) {
            String id = entry.getKey();
            String alignedSeq = entry.getValue();
            if (sequenceMap.containsKey(id)) {
                sequenceMap.get(id).setSeqAligned(alignedSeq);
            }
        }
    }


    private void runNextclade(Batch batch, Path originalSeqFastaPath) throws IOException, InterruptedException {
        // Execute Nextclade
        Path nextcladeCsvPath = workDir.resolve("nextclade.csv");
        String nextcladeCommand = "nextclade" +
                " --jobs=4" +
                " --input-fasta " + originalSeqFastaPath.toAbsolutePath() +
                " --output-csv " + nextcladeCsvPath.toAbsolutePath();
        Process nextcladeProcess = Runtime.getRuntime().exec(nextcladeCommand);
        boolean exited = nextcladeProcess.waitFor(20, TimeUnit.MINUTES);
        if (!exited) {
            nextcladeProcess.destroyForcibly();
            throw new RuntimeException("Nextclade timed out (after 20 minutes)");
        }
        if (nextcladeProcess.exitValue() != 0) {
            throw new RuntimeException("Nextclade exited with code " + nextcladeProcess.exitValue());
        }
        // Read Nextclade results from the generated csv file
        Reader nextcladeReader = new FileReader(nextcladeCsvPath.toFile());
        CSVParser csvRecords = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().parse(nextcladeReader);
        Map<String, Sequence> sequenceMap = new HashMap<>();
        for (Sequence sequence : batch.getSequences()) {
            sequenceMap.put(sequence.getGisaidEpiIsl(), sequence);
        }
        for (CSVRecord csvRecord : csvRecords) {
            String[] substitutions = csvRecord.get("aaSubstitutions").split(",");
            String[] deletions = csvRecord.get("aaDeletions").split(",");
            List<String> mutations = new ArrayList<>();
            mutations.addAll(Arrays.asList(substitutions));
            mutations.addAll(Arrays.asList(deletions));
            mutations = mutations.stream().map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList());
            sequenceMap.get(csvRecord.get("seqName"))
                    .setNextcladeClade(csvRecord.get("clade"))
                    .setNextcladeQcOverallScore(Utils.nullableFloatValue(csvRecord.get("qc.overallScore")))
                    .setNextcladeQcOverallStatus(csvRecord.get("qc.overallStatus"))
                    .setNextcladeTotalGaps(Utils.nullableIntegerValue(csvRecord.get("totalGaps")))
                    .setNextcladeTotalInsertions(Utils.nullableIntegerValue(csvRecord.get("totalInsertions")))
                    .setNextcladeTotalMissing(Utils.nullableIntegerValue(csvRecord.get("totalMissing")))
                    .setNextcladeTotalMutations(Utils.nullableIntegerValue(csvRecord.get("totalMutations")))
                    .setNextcladeTotalNonAcgtns(Utils.nullableIntegerValue(csvRecord.get("totalNonACGTNs")))
                    .setNextcladeTotalPcrPrimerChanges(Utils.nullableIntegerValue(csvRecord.get("totalPcrPrimerChanges")))
                    .setNextcladeAlignmentStart(Utils.nullableIntegerValue(csvRecord.get("alignmentStart")))
                    .setNextcladeAlignmentEnd(Utils.nullableIntegerValue(csvRecord.get("alignmentEnd")))
                    .setNextcladeAlignmentScore(Utils.nullableIntegerValue(csvRecord.get("alignmentScore")))
                    .setNextcladeQcMissingDataScore(Utils.nullableFloatValue(csvRecord.get("qc.missingData.score")))
                    .setNextcladeQcMissingDataStatus(csvRecord.get("qc.missingData.status"))
                    .setNextcladeQcMissingDataTotal(Utils.nullableIntegerValue(csvRecord.get("qc.missingData.totalMissing")))
                    .setNextcladeQcMixedSitesScore(Utils.nullableFloatValue(csvRecord.get("qc.mixedSites.score")))
                    .setNextcladeQcMixedSitesStatus(csvRecord.get("qc.mixedSites.status"))
                    .setNextcladeQcMixedSitesTotal(Utils.nullableIntegerValue(csvRecord.get("qc.mixedSites.totalMixedSites")))
                    .setNextcladeQcPrivateMutationsCutoff(Utils.nullableIntegerValue(csvRecord.get("qc.privateMutations.cutoff")))
                    .setNextcladeQcPrivateMutationsExcess(Utils.nullableIntegerValue(csvRecord.get("qc.privateMutations.excess")))
                    .setNextcladeQcPrivateMutationsScore(Utils.nullableFloatValue(csvRecord.get("qc.privateMutations.score")))
                    .setNextcladeQcPrivateMutationsStatus(csvRecord.get("qc.privateMutations.status"))
                    .setNextcladeQcPrivateMutationsTotal(Utils.nullableIntegerValue(csvRecord.get("qc.privateMutations.total")))
                    .setNextcladeQcSnpClustersClustered(csvRecord.get("qc.snpClusters.clusteredSNPs"))
                    .setNextcladeQcSnpClustersScore(Utils.nullableFloatValue(csvRecord.get("qc.snpClusters.score")))
                    .setNextcladeQcSnpClustersStatus(csvRecord.get("qc.snpClusters.status"))
                    .setNextcladeQcSnpClustersTotal(Utils.nullableIntegerValue(csvRecord.get("qc.snpClusters.totalSNPs")))
                    .setNextcladeErrors(csvRecord.get("errors"))
                    .setNextcladeMutations(mutations);
        }
    }


    private void writeToDatabase(Batch batch) throws SQLException {
        // If APPEND mode: Insert everything
        // If UPDATE mode + only metadata changed: update metadata and "updated_at" timestamp
        // If UPDATE mode + sequence changed: delete the old entry (including mutations) and re-insert everything
        List<Sequence> toUpdateMetadata = new ArrayList<>();
        List<Sequence> toDelete = new ArrayList<>();
        List<Sequence> toInsert = new ArrayList<>();
        for (Sequence sequence : batch.getSequences()) {
            if (sequence.getImportMode() == ImportMode.APPEND) {
                toInsert.add(sequence);
            } else if (sequence.getImportMode() == ImportMode.UPDATE) {
                if (!sequence.isSequenceChanged()) {
                    toUpdateMetadata.add(sequence);
                } else {
                    toDelete.add(sequence);
                    toInsert.add(sequence);
                }
            }
        }
        try (Connection conn = databasePool.getConnection()) {
            conn.setAutoCommit(false);

            // 1. Update the metadata
            String updateSequenceSql = """
                update gisaid_api_sequence
                set
                  updated_at = now(),
                  strain = ?,
                  virus = ?,
                  date = ?,
                  date_original = ?,
                  country = ?,
                  region_original = ?,
                  country_original = ?,
                  division = ?,
                  location = ?,
                  host = ?,
                  age = ?,
                  sex = ?,
                  pangolin_lineage = ?,
                  gisaid_clade = ?,
                  originating_lab = coalesce(?, originating_lab),
                  submitting_lab = coalesce(?, submitting_lab),
                  authors = coalesce(?, authors),
                  date_submitted = ?,
                  sampling_strategy = ?
                where gisaid_epi_isl = ?;
            """;
            try (PreparedStatement statement = conn.prepareStatement(updateSequenceSql)) {
                for (Sequence sequence : toUpdateMetadata) {
                    SubmitterInformation si = sequence.getSubmitterInformation();
                    statement.setString(1, sequence.getStrain());
                    statement.setString(2, sequence.getVirus());
                    statement.setDate(3, sequence.getDate() != null ? Date.valueOf(sequence.getDate()) : null);
                    statement.setString(4, sequence.getDateOriginal());
                    statement.setString(5, sequence.getCountry());
                    statement.setString(6, sequence.getRegionOriginal());
                    statement.setString(7, sequence.getCountryOriginal());
                    statement.setString(8, sequence.getDivision());
                    statement.setString(9, sequence.getLocation());
                    statement.setString(10, sequence.getHost());
                    statement.setObject(11, sequence.getAge());
                    statement.setString(12, sequence.getSex());
                    statement.setString(13, sequence.getPangolinLineage());
                    statement.setString(14, sequence.getGisaidClade());
                    statement.setString(15, si != null ? si.getOriginatingLab() : null);
                    statement.setString(16, si != null ? si.getSubmittingLab() : null);
                    statement.setString(17, si != null ? si.getAuthors() : null);
                    statement.setDate(18, sequence.getDateSubmitted() != null ? Date.valueOf(sequence.getDateSubmitted()) : null);
                    statement.setString(19, sequence.getSamplingStrategy());
                    statement.setString(20, sequence.getGisaidEpiIsl());
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            // 2. Delete sequences
            String deleteSequenceSql = """
                delete from gisaid_api_sequence
                where gisaid_epi_isl = ?;
            """;
            try (PreparedStatement statement = conn.prepareStatement(deleteSequenceSql)) {
                for (Sequence sequence : toDelete) {
                    statement.setString(1, sequence.getGisaidEpiIsl());
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();
            }

            // 3. Insert into gisaid_api_sequence
            String insertSequenceSql = """
                insert into gisaid_api_sequence (
                  updated_at,
                  gisaid_epi_isl, strain, virus, date, date_original, country, region_original, country_original,
                  division, location, host, age, sex, pangolin_lineage, gisaid_clade, originating_lab, submitting_lab, authors,
                  date_submitted, sampling_strategy,  seq_original, seq_aligned, nextclade_clade,
                  nextclade_qc_overall_score, nextclade_qc_overall_status, nextclade_total_gaps, nextclade_total_insertions,
                  nextclade_total_missing, nextclade_total_mutations, nextclade_total_non_acgtns,
                  nextclade_total_pcr_primer_changes, nextclade_alignment_start, nextclade_alignment_end,
                  nextclade_alignment_score, nextclade_qc_missing_data_score, nextclade_qc_missing_data_status,
                  nextclade_qc_missing_data_total, nextclade_qc_mixed_sites_score, nextclade_qc_mixed_sites_status,
                  nextclade_qc_mixed_sites_total, nextclade_qc_private_mutations_cutoff, nextclade_qc_private_mutations_excess,
                  nextclade_qc_private_mutations_score, nextclade_qc_private_mutations_status, nextclade_qc_private_mutations_total,
                  nextclade_qc_snp_clusters_clustered, nextclade_qc_snp_clusters_score, nextclade_qc_snp_clusters_status,
                  nextclade_qc_snp_clusters_total, nextclade_errors
                )
                values (
                  now(),
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?
                );
            """;
            try (PreparedStatement insertStatement = conn.prepareStatement(insertSequenceSql)) {
                for (Sequence sequence : toInsert) {
                    SubmitterInformation si = sequence.getSubmitterInformation();
                    insertStatement.setString(1, sequence.getGisaidEpiIsl());
                    insertStatement.setString(2, sequence.getStrain());
                    insertStatement.setString(3, sequence.getVirus());
                    insertStatement.setDate(4, sequence.getDate() != null ? Date.valueOf(sequence.getDate()) : null);
                    insertStatement.setString(5, sequence.getDateOriginal());
                    insertStatement.setString(6, sequence.getCountry());
                    insertStatement.setString(7, sequence.getRegionOriginal());
                    insertStatement.setString(8, sequence.getCountryOriginal());
                    insertStatement.setString(9, sequence.getDivision());
                    insertStatement.setString(10, sequence.getLocation());
                    insertStatement.setString(11, sequence.getHost());
                    insertStatement.setObject(12, sequence.getAge());
                    insertStatement.setString(13, sequence.getSex());
                    insertStatement.setString(14, sequence.getPangolinLineage());
                    insertStatement.setString(15, sequence.getGisaidClade());
                    insertStatement.setString(16, si != null ? si.getOriginatingLab() : null);
                    insertStatement.setString(17, si != null ? si.getSubmittingLab() : null);
                    insertStatement.setString(18, si != null ? si.getAuthors() : null);
                    insertStatement.setDate(19, sequence.getDateSubmitted() != null ? Date.valueOf(sequence.getDateSubmitted()) : null);
                    insertStatement.setString(20, sequence.getSamplingStrategy());
                    insertStatement.setString(21, sequence.getSeqOriginal());
                    insertStatement.setString(22, sequence.getSeqAligned());
                    insertStatement.setString(23, sequence.getNextcladeClade());
                    insertStatement.setObject(24, sequence.getNextcladeQcOverallScore());
                    insertStatement.setObject(25, sequence.getNextcladeQcOverallStatus());
                    insertStatement.setObject(26, sequence.getNextcladeTotalGaps());
                    insertStatement.setObject(27, sequence.getNextcladeTotalInsertions());
                    insertStatement.setObject(28, sequence.getNextcladeTotalMissing());
                    insertStatement.setObject(29, sequence.getNextcladeTotalMutations());
                    insertStatement.setObject(30, sequence.getNextcladeTotalNonAcgtns());
                    insertStatement.setObject(31, sequence.getNextcladeTotalPcrPrimerChanges());
                    insertStatement.setObject(32, sequence.getNextcladeAlignmentStart());
                    insertStatement.setObject(33, sequence.getNextcladeAlignmentEnd());
                    insertStatement.setObject(34, sequence.getNextcladeAlignmentScore());
                    insertStatement.setObject(35, sequence.getNextcladeQcMissingDataScore());
                    insertStatement.setString(36, sequence.getNextcladeQcMissingDataStatus());
                    insertStatement.setObject(37, sequence.getNextcladeQcMissingDataTotal());
                    insertStatement.setObject(38, sequence.getNextcladeQcMixedSitesScore());
                    insertStatement.setString(39, sequence.getNextcladeQcMixedSitesStatus());
                    insertStatement.setObject(40, sequence.getNextcladeQcMixedSitesTotal());
                    insertStatement.setObject(41, sequence.getNextcladeQcPrivateMutationsCutoff());
                    insertStatement.setObject(42, sequence.getNextcladeQcPrivateMutationsExcess());
                    insertStatement.setObject(43, sequence.getNextcladeQcPrivateMutationsScore());
                    insertStatement.setString(44, sequence.getNextcladeQcPrivateMutationsStatus());
                    insertStatement.setObject(45, sequence.getNextcladeQcPrivateMutationsTotal());
                    insertStatement.setString(46, sequence.getNextcladeQcSnpClustersClustered());
                    insertStatement.setObject(47, sequence.getNextcladeQcSnpClustersScore());
                    insertStatement.setString(48, sequence.getNextcladeQcSnpClustersStatus());
                    insertStatement.setObject(49, sequence.getNextcladeQcSnpClustersTotal());
                    insertStatement.setString(50, sequence.getNextcladeErrors());
                    insertStatement.addBatch();
                }
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }

            // 4. Insert into gisaid_api_sequence_nextclade_mutation_aa
            String insertAaMutationsSql = """
                insert into gisaid_api_sequence_nextclade_mutation_aa (gisaid_epi_isl, aa_mutation)
                values (?, ?);
            """;
            try (PreparedStatement insertStatement = conn.prepareStatement(insertAaMutationsSql)) {
                for (Sequence sequence : toInsert) {
                    if (sequence.getNextcladeMutations() == null) {
                        continue;
                    }
                    for (String mutation : sequence.getNextcladeMutations()) {
                        insertStatement.setString(1, sequence.getGisaidEpiIsl());
                        insertStatement.setString(2, mutation);
                        insertStatement.addBatch();
                    }
                }
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }

            // 5. Insert into gisaid_api_sequence_mutation_nucleotide
            String insertNucMutationsSql = """
                insert into gisaid_api_sequence_mutation_nucleotide (gisaid_epi_isl, position, mutation)
                values (?, ?, ?);
            """;
            try (PreparedStatement insertStatement = conn.prepareStatement(insertNucMutationsSql)) {
                for (Sequence sequence : toInsert) {
                    if (sequence.getNucleotideMutations() == null) {
                        continue;
                    }
                    for (NucleotideMutationFinder.Mutation mutation : sequence.getNucleotideMutations()) {
                        insertStatement.setString(1, sequence.getGisaidEpiIsl());
                        insertStatement.setInt(2, mutation.getPosition());
                        insertStatement.setString(3, String.valueOf(mutation.getMutation()));
                        insertStatement.addBatch();
                    }
                }
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }

            // 6. Commit
            conn.commit();
            conn.setAutoCommit(true);
        }
    }


    /**
     * If the sequence is submitted by us, update the sequence_identifier table.
     *
     * @return The weirdEntryReports and the number of added sequences from us
     */
    private Pair<List<WeirdEntryReport>, Integer> updateSequenceIdentifier(Batch batch) throws SQLException {
        List<WeirdEntryReport> weirdEntryReports = new ArrayList<>();
        List<Pair<Integer, String>> ethidAndGisaidId = new ArrayList<>();
        for (Sequence sequence : batch.getSequences()) {
            MaybeResult<Boolean> maybeIsOurs = ethzParser.isOurs(sequence);
            if (maybeIsOurs.getWeirdEntryReport() != null) {
                weirdEntryReports.add(maybeIsOurs.getWeirdEntryReport());
            }
            if (maybeIsOurs.isGoodEnough() && maybeIsOurs.getResult()) {
                MaybeResult<Integer> maybeEthid = ethzParser.parseEthid(sequence);
                if (maybeEthid.getWeirdEntryReport() != null) {
                    weirdEntryReports.add(maybeEthid.getWeirdEntryReport());
                }
                if (maybeEthid.isGoodEnough()) {
                    ethidAndGisaidId.add(new Pair<>(maybeEthid.getResult(), sequence.getGisaidEpiIsl()));
                }
            }
        }
        if (!ethidAndGisaidId.isEmpty()) {
            String updateSequenceIdentifierSql = """
                update sequence_identifier si
                set
                  gisaid_id = ?
                where
                    si.ethid = ?
                    and gisaid_id is null;
            """;
            try (Connection conn = databasePool.getConnection()) {
                try (PreparedStatement statement = conn.prepareStatement(updateSequenceIdentifierSql)) {
                    for (Pair<Integer, String> pair : ethidAndGisaidId) {
                        statement.setString(1, pair.getValue1());
                        statement.setInt(2, pair.getValue0());
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
            }
        }
        return new Pair<>(weirdEntryReports, ethidAndGisaidId.size());
    }

}
