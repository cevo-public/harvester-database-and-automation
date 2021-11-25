package ch.ethz.harvester.viollier;

import ch.ethz.harvester.core.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.javatuples.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;


public class ViollierMetadataReceiver extends SubProgram<ViollierMetadataReceiverConfig> {
    private static final Set<String> KNOWN_SEQUENCING_CENTER = new HashSet<>() {{
       add("viollier");
       add("gfb");
       add("fgcz");
       add("h2030");
    }};


    public ViollierMetadataReceiver() {
        super("ViollierMetadataReceiver", ViollierMetadataReceiverConfig.class);
    }


    @Override
    public void run(String[] args, ViollierMetadataReceiverConfig config) throws Exception {
        NotificationSystem notificationSystem
                = new NotificationSystemFactory().createNotificationSystemFromConfig(config.getNotification());
        try {
            if (!(notificationSystem instanceof SendAttachmentCapable)) {
                throw new RuntimeException("The provided notification system does not support attachments.");
            }
            Looper looper = new Looper(config.getLooper());
            while (looper.next()) {
                try (Connection conn = DatabaseService.openDatabaseConnection(config.getVineyard())) {
                    doWork(config, conn, (SendAttachmentCapable) notificationSystem);
                }
                looper.sleep();
            }
        } catch (Throwable e) {
            notificationSystem.sendReport(new ProgramCrashReport(e, "ViollierMetadataReceiver"));
            e.printStackTrace();
            System.exit(1);
        }
    }


    private void doWork(
            ViollierMetadataReceiverConfig config,
            Connection conn,
            SendAttachmentCapable notificationSystem
    ) throws IOException, SQLException {
        // Fetch the automation_state: It contains the files that already have been processed and those where
        // the processing has been started.
        AutomationState automationState;

        String fetchAutomationStateSql = """
            select state
            from automation_state
            where program_name = 'viollier_metadata_receiver';        
        """;
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(fetchAutomationStateSql)) {
                rs.next();
                String automationStateString = rs.getString("state");
                automationState = new ObjectMapper().readValue(automationStateString, AutomationState.class);
            }
        }


        // Get the names of the files in the sample_metadata directory
        Path inputDirPath = Path.of(config.getViollier().getSampleMetadataDirPath());
        List<Path> unprocessedFiles = new ArrayList<>();
        for (Path path : Files.list(inputDirPath).collect(Collectors.toList())) {
            String filename = path.getFileName().toString();
            if (!Files.isRegularFile(path)) {
                // We have no interest in directories or other weird files.
                continue;
            }
            if (filename.startsWith(".")) {
                // We have no interest in hidden files.
                continue;
            }
            if (!filename.endsWith(".csv")) {
                throw new RuntimeException("Found a file that does not end with .csv: " + path);
            }
            if (!automationState.getProcessedFiles().contains(filename)
                    && !automationState.getFilesInProcessing().contains(filename)) {
                unprocessedFiles.add(path);
                System.out.println("Found unprocessed file: " + filename);
            }
        }

        // Leave if all files have been processed or is being processed to automation_state
        if (unprocessedFiles.isEmpty()) {
            System.out.println("No new files - nothing to do.");
            return;
        }

        // Write into the database the names of the files that are going to be processed
        List<String> unprocessedFileNames = unprocessedFiles.stream()
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toList());
        System.out.println("Detected new file(s): " + String.join(", ", unprocessedFileNames));
        automationState.getFilesInProcessing().addAll(unprocessedFileNames);
        updateAutomationState(automationState, conn);

        // Fetch all existing plates from the database
        Set<String> existingPlates = fetchAllExistingPlates(conn);

        // The header names that we expect/tolerate
        Set<String> requiredColumns = new HashSet<>() {{
            add("Prescriber city");
            add("Zip code");
            add("Prescriber canton");
            add("Sequencing center");
            add("Sample number");
            add("Order date");
            add("PlateID");
            add("CT Wert");
            add("DeepWellLocation");
        }};
        Set<String> toleratedColumns = new HashSet<>() {{
            add("Author list for GISAID");
        }};

        List<Sample> allSamples = new ArrayList<>();
        Map<String, String> plateToSequencingCenter = new HashMap<>();
        Map<String, List<Sample>> sequencingCenterToSamples = new HashMap<>();
        for (Path unprocessedFile : unprocessedFiles) {
            // Read the new file
            Reader fileReader = new FileReader(unprocessedFile.toFile(), Charset.forName("Windows-1252"));
            CSVParser csvRecords = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().parse(fileReader);

            // Check: Are all required fields provided?
            HashSet<String> headerNames = new HashSet<>(csvRecords.getHeaderNames());
            Set<String> missingColumns = new HashSet<>(requiredColumns);
            missingColumns.removeAll(headerNames);
            Set<String> unexpectedColumns = new HashSet<>(headerNames);
            unexpectedColumns.removeAll(requiredColumns);
            unexpectedColumns.removeAll(toleratedColumns);
            if (!missingColumns.isEmpty() || !unexpectedColumns.isEmpty()) {
                throw new RuntimeException("We miss columns or found unexpected columns: "
                        + Arrays.toString(missingColumns.toArray()) + " --- "
                        + Arrays.toString(unexpectedColumns.toArray()));
            }

            // Create sample objects (including normalizations and sample-wise checks)
            List<Sample> samples = new ArrayList<>();
            for (CSVRecord csvRecord : csvRecords) {
                Integer sampleNumber = Utils.nullableIntegerValue(csvRecord.get("Sample number"));
                String canton = Utils.nullableBlankToNull(csvRecord.get("Prescriber canton"));
                String city = Utils.nullableBlankToNull(csvRecord.get("Prescriber city"));
                String zipCode = Utils.nullableBlankToNull(csvRecord.get("Zip code"));
                String sequencingCenter = Utils.nullableBlankToNull(csvRecord.get("Sequencing center"));
                String dateString = Utils.nullableBlankToNull(csvRecord.get("Order date"));
                Integer ct = Utils.nullableIntegerValue(csvRecord.get("CT Wert"));
                String viollierPlateName = csvRecord.get("PlateID");
                String wellPosition = csvRecord.get("DeepWellLocation");

                // The important metadata are only allowed to be missing if the whole row is actually empty.
                if (sampleNumber == null || sampleNumber == 0 || canton == null || dateString == null) {
                    if ((sampleNumber != null && sampleNumber != 0) || canton != null || city != null || zipCode != null
                            || dateString != null || ct != null) {
                        throw new RuntimeException("Important metadata are missing.");
                    }
                    continue;
                }

                // Detect date format
                LocalDate orderDate;
                if (dateString.matches("[0-9]{2}.[0-9]{2}.[0-9]{4}")) {
                    orderDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd.MM.yyyy"));
                } else if (dateString.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                    orderDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                } else {
                    throw new RuntimeException("Unrecognized date format: " + dateString);
                }

                Sample sample = new Sample()
                        .setSampleNumber(sampleNumber)
                        .setCanton(canton)
                        .setCity(city)
                        .setZipCode(zipCode)
                        .setSequencingCenter(sequencingCenter)
                        .setOrderDate(orderDate)
                        .setCt(ct)
                        .setViollierPlateName(viollierPlateName)
                        .setWellPosition(wellPosition);
                normalizeSample(sample);
                boolean sampleOk = checkSample(sample);
                if (!sampleOk) {
                    throw new RuntimeException("Sample failed the checks: " + sample.getSampleNumber());
                }
                samples.add(sample);
            }

            // Perform plate-wise checks
            Map<String, Set<String>> plateAndWells = new HashMap<>();
            for (Sample sample : samples) {
                // Check: Are there multiple samples assigned to the same plate/well?
                plateAndWells.putIfAbsent(sample.getViollierPlateName(), new HashSet<>());
                Set<String> wellsOfPlate = plateAndWells.get(sample.getViollierPlateName());
                if (wellsOfPlate.contains(sample.getWellPosition())) {
                    throw new RuntimeException("Multiple samples are assigned to the sample plate+well.");
                }
                wellsOfPlate.add(sample.getWellPosition());

                // Check: Is the plate already in the database?
                if (existingPlates.contains(sample.getViollierPlateName())) {
                    continue;
                    // We receive a lot of files with duplicated plates. We will not import the same sample twice.
                    //throw new RuntimeException("The plate is already in the database.");
                }

                // Check: Are samples of the same plate being assigned to different sequencing centers?
                plateToSequencingCenter.putIfAbsent(sample.getViollierPlateName(), sample.getSequencingCenter());
                if (!sample.getSequencingCenter().equals(plateToSequencingCenter.get(sample.getViollierPlateName()))) {
                    throw new RuntimeException("The same plate was assigned to multiple sequencing centers");
                }
                sequencingCenterToSamples.putIfAbsent(sample.getSequencingCenter(), new ArrayList<>());
                sequencingCenterToSamples.get(sample.getSequencingCenter()).add(sample);

                allSamples.add(sample);
            }
        }

        // Leave if there are no new samples to import
        if (allSamples.isEmpty()) {
            System.out.println("We found new files but no new samples - leaving.");
            return;
        }

        // Write to the database
        writeToDatabase(conn, allSamples, plateToSequencingCenter);

        // Create the sample list for the sequencing centers (or just for us internally)
        List<ReportAttachment> reportAttachments = new ArrayList<>();
        for (Map.Entry<String, List<Sample>> entry : sequencingCenterToSamples.entrySet()) {
            String sequencingCenter = entry.getKey();
            List<Sample> samples = entry.getValue();
            String csv = switch (sequencingCenter) {
                case "gfb" -> formatCsvFileForGfb(samples);
                case "fgcz" -> formatCsvFileForFgcz(samples);
                case "h2030" -> formatCsvFileForH2030(samples);
                case "viollier" -> formatCsvFileForSamplesSequencedAtViollierJustForUs(samples);
                default -> throw new RuntimeException("Unexpected error: Found an unexpected sequencing center after validation: "
                        + sequencingCenter);
            };
            reportAttachments.add(new ReportAttachment(
                    "sars-cov-2_samples_" + sequencingCenter + "_" + LocalDate.now() + ".csv",
                    csv.getBytes(StandardCharsets.UTF_8),
                    "text/csv"
            ));
        }

        // Send the sample list by email
        NewSamplesReport newSamplesReport = new NewSamplesReport(
                unprocessedFiles.stream().map(p -> p.getFileName().toString()).collect(Collectors.toList()),
                new ArrayList<>(plateToSequencingCenter.keySet()),
                sequencingCenterToSamples.values().stream().reduce(0, (sum, ls) -> sum + ls.size(), Integer::sum),
                new ArrayList<>(sequencingCenterToSamples.keySet()),
                reportAttachments
        );
        List<String> additionalRecipients = new ArrayList<>(config.getViollier().getAdditionalRecipients());
        if (sequencingCenterToSamples.containsKey("gfb")) {
            additionalRecipients.addAll(config.getViollier().getGfbNotificationRecipients());
        }
        if (sequencingCenterToSamples.containsKey("fgcz")) {
            additionalRecipients.addAll(config.getViollier().getFgczNotificationRecipients());
        }
        if (sequencingCenterToSamples.containsKey("h2030")) {
            additionalRecipients.addAll(config.getViollier().getH2030NotificationRecipients());
        }
        notificationSystem.sendReportWithAttachment(newSamplesReport, additionalRecipients);

        // Update automation state: Write into the database the names of the files that have been processed
        automationState.getFilesInProcessing().removeAll(unprocessedFileNames);
        automationState.getProcessedFiles().addAll(unprocessedFileNames);
        updateAutomationState(automationState, conn);

        System.out.println("Finished!");
    }

    private boolean checkSample(Sample sample) {
        if (!KNOWN_SEQUENCING_CENTER.contains(sample.getSequencingCenter())) {
            return false;
        }
        return true;
        // TODO This is a good place to perform more sanity checks: e.g., check order date, canton, ct value.
    }

    private void normalizeSample(Sample sample) {
        if ("gfb".equals(sample.getSequencingCenter().toLowerCase())) {
            sample.setSequencingCenter("gfb");
        } else if ("fgcz".equals(sample.getSequencingCenter().toLowerCase())) {
            sample.setSequencingCenter("fgcz");
        } else if (sample.getSequencingCenter().toLowerCase().contains("viollier")) {
            sample.setSequencingCenter("viollier");
        } else if (sample.getSequencingCenter().toLowerCase().contains("health2030")) {
            sample.setSequencingCenter("h2030");
        }
        sample.setViollierPlateName(sample.getViollierPlateName().toLowerCase());
        // Normalize the well position name: upper-case and (A01 -> A1)
        Pair<String, Integer> wellPositionParts = splitWellPosition(sample.getWellPosition().toUpperCase());
        sample.setWellPosition(wellPositionParts.getValue0() + wellPositionParts.getValue1());
    }

    private void updateAutomationState(AutomationState automationState, Connection conn)
            throws SQLException, JsonProcessingException {
        String updateAutomationStateSql = """
            update automation_state
            set state = ?
            where program_name = 'viollier_metadata_receiver';
        """;
        try (PreparedStatement statement = conn.prepareStatement(updateAutomationStateSql)) {
            statement.setString(1, new ObjectMapper().writeValueAsString(automationState));
            statement.execute();
        }
    }

    private Set<String> fetchAllExistingPlates(Connection conn) throws SQLException {
        String fetchSql = """
            select viollier_plate_name
            from viollier_plate;
        """;
        Set<String> plates = new HashSet<>();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(fetchSql)) {
                while (rs.next()) {
                    plates.add(rs.getString("viollier_plate_name"));
                }
            }
        }
        return plates;
    }

    private String formatCsvFileForGfb(List<Sample> samples) {
        samples.sort((a, b) -> {
            // First sort by plate
            int plateNameCompare = a.getViollierPlateName().compareTo(b.getViollierPlateName());
            if (plateNameCompare != 0) {
                return plateNameCompare;
            }
            // Then A1-H1, A2-H2, ...
            Pair<String, Integer> wellPartsA = splitWellPosition(a.getWellPosition());
            Pair<String, Integer> wellPartsB = splitWellPosition(b.getWellPosition());
            if (!wellPartsA.getValue1().equals(wellPartsB.getValue1())) {
                return wellPartsA.getValue1().compareTo(wellPartsB.getValue1());
            }
            return wellPartsA.getValue0().compareTo(wellPartsB.getValue0());
        });
        StringWriter writer = new StringWriter();
        try {
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader("ethid", "order_date", "ct", "viollier_plate_name", "well_position", "id_and_well"));
            for (Sample sample : samples) {
                csvPrinter.printRecord(
                        sample.getSampleNumber(),
                        sample.getOrderDate(),
                        sample.getCt(),
                        sample.getViollierPlateName(),
                        sample.getWellPosition(),
                        sample.getSampleNumber() + "_" + sample.getWellPosition()
                );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer.toString();
    }

    private String formatCsvFileForFgcz(List<Sample> samples) {
        return formatCsvFileForGfb(samples);
    }

    private String formatCsvFileForH2030(List<Sample> samples) {
        return formatCsvFileForGfb(samples);
    }

    private String formatCsvFileForSamplesSequencedAtViollierJustForUs(List<Sample> samples) {
        return formatCsvFileForGfb(samples);
    }

    private Pair<String, Integer> splitWellPosition(String wellPosition) {
        String part1 = wellPosition.substring(0, 1);
        int part2 = Integer.parseInt(wellPosition.substring(1));
        return new Pair<>(part1, part2);
    }

    private void writeToDatabase(
            Connection conn,
            List<Sample> samples,
            Map<String, String> plateToSequencingCenter
    ) throws SQLException {
        conn.setAutoCommit(false);

        // Add to viollier_test (if the sample is not already in the database)
        String insertVtSql = """
            insert into viollier_test (sample_number, ethid, order_date, zip_code, city, canton, is_positive)
            values (?, ?, ?, ?, ?, ?, true)
            on conflict do nothing;       
        """;
        try (PreparedStatement statement = conn.prepareStatement(insertVtSql)) {
            for (Sample sample : samples) {
                statement.setInt(1, sample.getSampleNumber());
                statement.setInt(2, sample.getSampleNumber());
                statement.setDate(3, Date.valueOf(sample.getOrderDate()));
                statement.setString(4, sample.getZipCode());
                statement.setString(5, sample.getCity());
                statement.setString(6, sample.getCanton());
                statement.addBatch();
            }
            statement.executeBatch();
            statement.clearBatch();
        }

        // Add to viollier_plate
        String insertVp = """
            insert into viollier_plate (viollier_plate_name, left_viollier_date, sequencing_center, comment)
            values (?, ?, ?, 'The left_viollier_date might be inaccurate. It contains the date when we received the file which might not be the same date when the plate was extracted and sent.');
        """;
        try (PreparedStatement statement = conn.prepareStatement(insertVp)) {
            for (Map.Entry<String, String> plateAndSeqCenter : plateToSequencingCenter.entrySet()) {
                String plate = plateAndSeqCenter.getKey();
                String sequencingCenter = plateAndSeqCenter.getValue();
                statement.setString(1, plate);
                statement.setDate(2, Date.valueOf(LocalDate.now()));
                statement.setString(3, sequencingCenter);
                statement.addBatch();
            }
            statement.executeBatch();
            statement.clearBatch();
        }

        // Add to viollier_test_viollier_plate
        String insertVtvp = """
            insert into viollier_test__viollier_plate (sample_number, viollier_plate_name, well_position, e_gene_ct, seq_request)
            values (?, ?, ?, ?, true);
        """;
        try (PreparedStatement statement = conn.prepareStatement(insertVtvp)) {
            for (Sample sample : samples) {
                statement.setInt(1, sample.getSampleNumber());
                statement.setString(2, sample.getViollierPlateName());
                statement.setString(3, sample.getWellPosition());
                statement.setObject(4, sample.getCt());
                statement.addBatch();
            }
            statement.executeBatch();
            statement.clearBatch();
        }

        conn.commit();
        conn.setAutoCommit(true);
    }
}
