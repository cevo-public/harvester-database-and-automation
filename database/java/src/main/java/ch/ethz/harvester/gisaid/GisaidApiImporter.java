package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.core.*;
import ch.ethz.harvester.general.NucleotideMutationFinder;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.tukaani.xz.XZInputStream;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class GisaidApiImporter extends SubProgram<GisaidProgramConfig> {

    private ComboPooledDataSource databasePool;


    public GisaidApiImporter() {
        super("GisaidApiImporter", GisaidProgramConfig.class);
    }


    @Override
    public void run(String[] args, GisaidProgramConfig config) throws Exception {
        NotificationSystem notificationSystem = new NotificationSystemFactory()
                .createNotificationSystemFromConfig(config.getNotification());
        GlobalProxyManager.setProxyFromConfig(config.getHttpProxy());

        try {
            mainWork(config, notificationSystem);
            // TODO Sometimes, the system does not entirely shut down. Maybe there is an external process that is
            //   not getting closed? Will an explicit System.exit() help?
            System.exit(0);
        } catch (Throwable e) {
            notificationSystem.sendReport(new ProgramCrashReport(e, "GisaidApiImporter"));
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     *
     * @return Whether the program terminates successfully.
     */
    private boolean mainWork(GisaidProgramConfig config, NotificationSystem notificationSystem)
            throws IOException, SQLException, ParseException, InterruptedException {
        LocalDateTime startTime = LocalDateTime.now();
        GisaidProgramConfig.GisaidApiImporterConfig programConfig = config.getGisaidApiImporter();
        GisaidProgramConfig.GisaidConfig gisaidConfig = config.getGisaid();

        // There is an append and an update mode. The append mode only adds new sequences. It skips a sequence
        // if its GISAID EPI ISL is already in the database. The update mode will add new sequences and compare every
        // existing sequence from the data package with the entry in the database. It performs an update if something
        // has changed. Further, the update mode will delete all sequences from the database that are not in the data
        // package anymore.

        String workDirArg = programConfig.getWorkdir();
        Path geoLocationRulesFile = Path.of(programConfig.getGeoLocationRulesFile());
        GeoLocationMapper geoLocationMapper = new GeoLocationMapper(geoLocationRulesFile);
        String gisaidApiUrlArg = gisaidConfig.getUrl();
        String gisaidApiUsername = gisaidConfig.getUsername();
        String gisaidApiPassword = gisaidConfig.getPassword();
        ImportMode importMode = programConfig.getImportMode();
        boolean updateSubmitterInformation = importMode == ImportMode.UPDATE
                && (boolean) programConfig.getUpdateSubmitterInformation();
        int numberWorkers = programConfig.getNumberWorkers();
        int batchSize = programConfig.getBatchSize();
        Path workDir = Path.of(workDirArg);

        /* Preparations */
        databasePool = DatabaseService.createDatabaseConnectionPool(config.getVineyard());

        /* Check that everything is ready */

        // Check that the $workDir exists, is empty and is writable.
        // TODO Use a temporary directory if no $workDir is provided
        if (!Files.exists(workDir) || !Files.isDirectory(workDir)) {
            throw new IllegalArgumentException("The provided workdir does not exist.");
        }
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(workDir)) {
            if (directory.iterator().hasNext()) {
                throw new IllegalArgumentException("The provided workdir is not empty.");
            }
        }
        try {
            Files.writeString(workDir.resolve("test-file.txt"), "This is a simple test string.\n");
        } catch (IOException e) {
            throw new IllegalArgumentException("The provided workdir is not writable.");
        }

        // Check database connection
        try (Connection conn = databasePool.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery("select * from gisaid_country limit 1;")) {
                    rs.next();
                }
            }
        } catch (SQLException e) {
            System.err.println("The database connection is not working.");
            throw e;
        }

        // Check access to the public internet
        try {
            URL url = new URL("https://www.google.com");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.connect();
            connection.disconnect();
        } catch (IOException e) {
            System.err.println("The public internet cannot be reached (or google.com is down).");
            throw e;
        }

        // Check that all required external programs can be found
        //   - mafft
        //   - Nextclade


        /* Main part */

        // Fetch the mapping from the GISAID country names to iso_country
        Map<String, String> gisaidCountryMap = loadGisaidCountryMap();

        // Write the reference sequence to a fasta
        Path referenceFasta = workDir.resolve("reference.fasta");
        Files.writeString(referenceFasta, ">REFERENCE\n" + Reference.REFERENCE + "\n\n");

        // Download the compressed data (note: the data will not be fully de-compressed but directly read from the
        // compressed archive)
        Path gisaidDataFile = workDir.resolve("provision.json.xz");
        try {
            downloadDataPackage(
                    new URL(gisaidApiUrlArg),
                    gisaidApiUsername,
                    gisaidApiPassword,
                    workDir.resolve("provision.json.xz")
            );
        } catch (IOException e) {
            System.err.println("provision.json.xz could not be downloaded from GISAID");
            throw e;
        }

        // Read the first 10 lines and check that (1) all required attributes are present, and (2) if unexpected
        // attributes were found or expected (but non-required) attributes were not found, send a notification email.
        Set<String> requiredFields = new HashSet<>(Arrays.asList( // TODO lab information is still missing.
                "covv_virus_name",
                "covv_patient_age",
                "covv_gender",
                "covv_location",
                "covv_lineage",
                "covv_type",
                "covv_collection_date",
                "covv_accession_id",
                "sequence",
                "pangolin_lineages_version",
                "covv_clade",
                "covv_sampling_strategy",
                "covv_host",
                "covv_subm_date",
                "gc_content"
        ));
        Set<String> expectedFields = new HashSet<>(requiredFields);
        expectedFields.addAll(Arrays.asList(
                "covsurver_prot_mutations",
                "is_high_coverage",
                "sequence_length",
                "is_reference",
                "n_content",
                "covsurver_uniquemutlist",
                "is_complete",
                "covv_variant",
                "covv_add_host_info"
        ));
        Set<String> missingFields = new HashSet<>();
        Set<String> missingRequiredFields = new HashSet<>();
        Set<String> additionalFields = new HashSet<>();

        BufferedInputStream compressedIn = new BufferedInputStream(new FileInputStream(gisaidDataFile.toFile()));
        XZInputStream decompressedIn = new XZInputStream(compressedIn);
        BufferedReader gisaidReader = new BufferedReader(new InputStreamReader(decompressedIn, StandardCharsets.UTF_8));
        for (int i = 0; i < 10; i++) {
            String line = gisaidReader.readLine();
            JSONObject json = (JSONObject) new JSONParser().parse(line);
            Set<String> foundFields = json.keySet();
            Set<String> _missingFields = new HashSet<>(expectedFields);
            _missingFields.removeAll(foundFields);
            Set<String> _missingRequiredFields = new HashSet<>(requiredFields);
            _missingRequiredFields.removeAll(foundFields);
            Set<String> _additionalFields = new HashSet<>(foundFields);
            _additionalFields.removeAll(expectedFields);
            missingFields.addAll(_missingFields);
            missingRequiredFields.addAll(_missingRequiredFields);
            additionalFields.addAll(_additionalFields);
        }
        compressedIn.close();
        if (!missingFields.isEmpty() || !additionalFields.isEmpty()) {
            notificationSystem.sendReport(new UnexpectedDataReport(missingFields, missingRequiredFields, additionalFields));
            if (!missingRequiredFields.isEmpty()) {
                return false;
            }
        }

        // Load the list of all GISAID EPI ISL from the database.
        String loadExistingIdsSql = """
            select gisaid_epi_isl
            from gisaid_api_sequence;
        """;
        Set<String> existingGisaidEpiIsls = new HashSet<>();
        try (Connection conn = databasePool.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(loadExistingIdsSql)) {
                    while (rs.next()) {
                        existingGisaidEpiIsls.add(rs.getString("gisaid_epi_isl"));
                    }
                }
            }
        }

        // Create an instance of the NucleotideMutationFinder
        NucleotideMutationFinder nucleotideMutationFinder;
        try (Connection conn = databasePool.getConnection()) {
            String referenceGenome = NucleotideMutationFinder.loadReferenceGenome(conn);
            Set<Integer> maskSites = NucleotideMutationFinder.loadMaskSites(conn);
            nucleotideMutationFinder = new NucleotideMutationFinder(referenceGenome, maskSites);
        }

        // Create a queue to store batches and start workers to process them.
        ExhaustibleBlockingQueue<Batch> gisaidBatchQueue = new ExhaustibleLinkedBlockingQueue<>(Math.max(4, numberWorkers / 2));
        final ConcurrentLinkedQueue<BatchReport> batchReports = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Exception> unhandledExceptions = new ConcurrentLinkedQueue<>();
        final AtomicBoolean emergencyBrake = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(numberWorkers);

        for (int i = 0; i < numberWorkers; i++) {
            //Create a work directory for the worker
            Path workerWorkDir = workDir.resolve("worker-" + i);
            Files.createDirectory(workerWorkDir);

            // Start worker
            final int finalI = i;
            executor.submit(() -> {
                BatchProcessingWorker worker = new BatchProcessingWorker(
                        finalI,
                        workerWorkDir,
                        referenceFasta,
                        databasePool,
                        importMode,
                        updateSubmitterInformation,
                        nucleotideMutationFinder
                );
                while (!emergencyBrake.get() && (!gisaidBatchQueue.isExhausted() || !gisaidBatchQueue.isEmpty())) {
                    try {
                        Batch batch = gisaidBatchQueue.poll(5, TimeUnit.SECONDS);
                        if (batch == null) {
                            continue;
                        }
                        BatchReport batchReport = worker.run(batch);
                        batchReports.add(batchReport);
                    } catch (InterruptedException e) {
                        // When the emergency brake is pulled, it is likely that a worker will be interrupted. This is
                        // normal and does not constitute an additional error.
                        if (!emergencyBrake.get()) {
                            unhandledExceptions.add(e);
                        }
                    } catch (Exception e) {
                        unhandledExceptions.add(e);
                        emergencyBrake.set(true);
                        return;
                    }
                }
            });
        }

        // Iterate through the downloaded data package. If APPEND MODE: exclude all sequences that are already in the
        // database. Group the sequences into batches of $batchSize samples and put the batches into the
        // $gisaidBatchQueue. All found GISAID EPI ISL will be collected in a list.
        List<Sequence> batchEntries = new ArrayList<>();
        compressedIn = new BufferedInputStream(new FileInputStream(gisaidDataFile.toFile()));
        decompressedIn = new XZInputStream(compressedIn);
        gisaidReader = new BufferedReader(new InputStreamReader(decompressedIn, StandardCharsets.UTF_8));
        String line;
        int entriesInDataPackage = 0;
        int processedEntries = 0;
        Set<String> gisaidEpiIslInDataPackage = new HashSet<>();
        while ((line = gisaidReader.readLine()) != null) {
            if (emergencyBrake.get()) {
                break;
            }
            entriesInDataPackage++;
            if (entriesInDataPackage % 10000 == 0) {
                System.out.println("[main] Read " + entriesInDataPackage + " in the data package");
            }
            try {
                JSONObject json = (JSONObject) new JSONParser().parse(line);
                String gisaidEpiIsl = (String) json.get("covv_accession_id");
                gisaidEpiIslInDataPackage.add(gisaidEpiIsl);
                if (importMode == ImportMode.APPEND) {
                    if (existingGisaidEpiIsls.contains(gisaidEpiIsl)) {
                        continue;
                    }
                }
                Sequence sequence = parseDataPackageLine(json, gisaidCountryMap, geoLocationMapper);
                batchEntries.add(sequence);
                processedEntries++;
            } catch (ParseException e) {
                System.err.println("JSON parsing failed!");
                throw e;
            }
            if (batchEntries.size() >= batchSize) {
                Batch batch = new Batch(batchEntries);
                while (!emergencyBrake.get()) {
                    System.out.println("[main] Try adding a batch");
                    boolean success = gisaidBatchQueue.offer(batch, 5, TimeUnit.SECONDS);
                    if (success) {
                        System.out.println("[main] Batch added");
                        break;
                    }
                }
                batchEntries = new ArrayList<>();
            }
        }
        if (!emergencyBrake.get() && !batchEntries.isEmpty()) {
            Batch lastBatch = new Batch(batchEntries);
            while (!emergencyBrake.get()) {
                System.out.println("[main] Try adding a batch");
                boolean success = gisaidBatchQueue.offer(lastBatch, 5, TimeUnit.SECONDS);
                if (success) {
                    System.out.println("[main] Batch added");
                    break;
                }
            }
            batchEntries = null;
        }
        gisaidBatchQueue.setExhausted(true);

        // If someone pulled the emergency brake, collect some information and send a notification email.
        if (emergencyBrake.get()) {
            System.err.println("Emergency exit!");
            executor.shutdown();
            boolean terminated = executor.awaitTermination(3, TimeUnit.MINUTES);
            if (!terminated) {
                executor.shutdownNow();
            }
        } else {
            // Wait until all batches are finished.
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }

        // Deletions will also be performed by the APPEND mode because it's important to perform deletions on a
        // daily basis to prevent redundant data. Also, deletions are fast.
        int deleted = 0;
        if (!emergencyBrake.get()) {
            System.out.println("[main] Deleting removed sequences");
            Set<String> toDelete = new HashSet<>(existingGisaidEpiIsls);
            toDelete.removeAll(gisaidEpiIslInDataPackage);
            deleteSequences(toDelete);
            deleted = toDelete.size();
        }

        // Refresh materialized views
        if (!emergencyBrake.get()) {
            System.out.println("[main] Refreshing materialized views");
            refreshMaterializedViews();
        }

        // Merge the BatchReports to a report and send it by email.
        System.out.println("[main] Preparing final report");
        BatchReport mergedBatchReport = mergeBatchReports(new ArrayList<>(batchReports));
        boolean success = unhandledExceptions.isEmpty()
                && mergedBatchReport.getFailedEntries() < 0.05 * processedEntries;
        FinalReport finalReport = new FinalReport()
                .setSuccess(success)
                .setImportMode(importMode)
                .setStartTime(startTime)
                .setEndTime(LocalDateTime.now())
                .setEntriesInDataPackage(entriesInDataPackage)
                .setProcessedEntries(processedEntries)
                .setAddedEntries(mergedBatchReport.getAddedEntries())
                .setUpdatedTotalEntries(mergedBatchReport.getUpdatedTotalEntries())
                .setUpdatedMetadataEntries(mergedBatchReport.getUpdatedMetadataEntries())
                .setUpdatedSequenceEntries(mergedBatchReport.getUpdatedSequenceEntries())
                .setDeletedEntries(deleted)
                .setAddedEntriesFromUs(mergedBatchReport.getAddedEntriesFromUs())
                .setFailedEntries(mergedBatchReport.getFailedEntries())
                .setWeirdEntryReports(mergedBatchReport.getWeirdEntryReports())
                .setUnhandledExceptions(new ArrayList<>(unhandledExceptions));
        notificationSystem.sendReport(finalReport);

        // Clean up the work directory
        try (DirectoryStream<Path> directory = Files.newDirectoryStream(workDir)) {
            for (Path path : directory) {
                Files.delete(path);
            }
        }

        return success;
    }


    private Map<String, String> loadGisaidCountryMap() throws SQLException {
        String loadMappingSql = """
            select gisaid_country, iso_country
            from gisaid_country;
        """;
        Map<String, String> map = new HashMap<>();
        try (Connection conn = databasePool.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery(loadMappingSql)) {
                    while (rs.next()) {
                        map.put(rs.getString("gisaid_country"), rs.getString("iso_country"));
                    }
                }
            }
        }
        return map;
    }


    private void downloadDataPackage(URL url, String username, String password, Path outputPath) throws IOException {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        String authHeaderValue = "Basic " + new String(encodedAuth);
        HttpURLConnection gisaidApiConnection = (HttpURLConnection) url.openConnection();
        gisaidApiConnection.setRequestProperty("Authorization", authHeaderValue);
        ReadableByteChannel readableByteChannel = Channels.newChannel(gisaidApiConnection.getInputStream());
        FileOutputStream fileOutputStream = new FileOutputStream(outputPath.toFile());
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        fileChannel.close();
        fileOutputStream.close();
    }


    private Sequence parseDataPackageLine(
            JSONObject json,
            Map<String, String> gisaidCountryMap,
            GeoLocationMapper geoLocationMapper
    ) {
        // Parse date
        String dateOriginal = (String) json.get("covv_collection_date");
        LocalDate date = null;
        try {
            if (dateOriginal != null) {
                date = LocalDate.parse(dateOriginal);
            }
        } catch (DateTimeParseException ignored) {
        }

        // Parse geo data
        String locationString = (String) json.get("covv_location");
        String country = null;
        GeoLocation geoLocation;
        if (locationString != null) {
            List<String> locationParts = Arrays.stream(locationString.split("/"))
                    .map(String::trim)
                    .collect(Collectors.toList());
            GeoLocation gisaidDirtyLocation = new GeoLocation();
            if (locationParts.size() > 0) {
                gisaidDirtyLocation.setRegion(locationParts.get(0));
            }
            if (locationParts.size() > 1) {
                gisaidDirtyLocation.setCountry(locationParts.get(1));
            }
            if (locationParts.size() > 2) {
                gisaidDirtyLocation.setDivision(locationParts.get(2));
            }
            if (locationParts.size() > 3) {
                gisaidDirtyLocation.setLocation(locationParts.get(3));
            }
            geoLocation = geoLocationMapper.resolve(gisaidDirtyLocation);
            if (geoLocation.getCountry() != null) {
                country = gisaidCountryMap.get(geoLocation.getCountry());
            }
        } else {
            geoLocation = new GeoLocation();
        }

        // Parse age
        String ageString = (String) json.get("covv_patient_age");
        Integer age = Utils.nullableIntegerValue(ageString);

        // Parse sex
        String sexString = (String) json.get("covv_gender");
        if ("male".equalsIgnoreCase(sexString)) {
            sexString = "Male";
        } else if ("female".equalsIgnoreCase(sexString)) {
            sexString = "Female";
        } else {
            sexString = null;
        }

        // Parse date_submitted
        LocalDate dateSubmitted = Utils.nullableLocalDateValue((String) json.get("covv_subm_date"));

        return new Sequence()
                .setGisaidEpiIsl((String) json.get("covv_accession_id"))
                .setStrain((String) json.get("covv_virus_name"))
                .setVirus((String) json.get("covv_type"))
                .setDate(date)
                .setDateOriginal(dateOriginal)
                .setCountry(country)
                .setRegionOriginal(geoLocation.getRegion())
                .setCountryOriginal(geoLocation.getCountry())
                .setDivision(geoLocation.getDivision())
                .setLocation(geoLocation.getLocation())
                .setHost((String) json.get("covv_host"))
                .setAge(age)
                .setSex(sexString)
                .setPangolinLineage((String) json.get("covv_lineage"))
                .setGisaidClade((String) json.get("covv_clade"))
                .setDateSubmitted(dateSubmitted)
                .setSamplingStrategy((String) json.get("covv_sampling_strategy"))
                .setSeqOriginal((String) json.get("sequence"));
    }


    private void deleteSequences(Set<String> gisaidEpiIslToDelete) throws SQLException {
        String sql = """
            delete from gisaid_api_sequence where gisaid_epi_isl = ?;
        """;
        try (Connection conn = databasePool.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                for (String id : gisaidEpiIslToDelete) {
                    statement.setString(1, id);
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.clearBatch();;
            }
            conn.commit();
            conn.setAutoCommit(true);
        }
    }


    private BatchReport mergeBatchReports(List<BatchReport> batchReports) {
        int addedEntries = 0;
        int updatedTotalEntries = 0;
        int updatedMetadataEntries = 0;
        int updatedSequenceEntries = 0;
        int addedEntriesFromUs = 0;
        int failedEntries = 0;
        List<WeirdEntryReport> weirdEntryReports = new ArrayList<>();
        for (BatchReport batchReport : batchReports) {
            addedEntries += batchReport.getAddedEntries();
            updatedTotalEntries += batchReport.getUpdatedTotalEntries();
            updatedMetadataEntries += batchReport.getUpdatedMetadataEntries();
            updatedSequenceEntries += batchReport.getUpdatedSequenceEntries();
            addedEntriesFromUs += batchReport.getAddedEntriesFromUs();
            failedEntries += batchReport.getFailedEntries();
            weirdEntryReports.addAll(batchReport.getWeirdEntryReports());
        }
        return new BatchReport()
                .setAddedEntries(addedEntries)
                .setUpdatedTotalEntries(updatedTotalEntries)
                .setUpdatedMetadataEntries(updatedMetadataEntries)
                .setUpdatedSequenceEntries(updatedSequenceEntries)
                .setAddedEntriesFromUs(addedEntriesFromUs)
                .setFailedEntries(failedEntries)
                .setWeirdEntryReports(weirdEntryReports);
    }


    private void refreshMaterializedViews() throws SQLException {
        try (Connection conn = databasePool.getConnection()) {
            conn.setAutoCommit(false);
            try (Statement statement = conn.createStatement()) {
                statement.execute("select refresh_all_mv();");
            }
            conn.commit();
            conn.setAutoCommit(true);
        }
    }
}
