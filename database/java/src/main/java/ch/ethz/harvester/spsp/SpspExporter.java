package ch.ethz.harvester.spsp;

import ch.ethz.harvester.core.*;
import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SpspExporter extends SubProgram<EmptyConfig> {

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


    public SpspExporter() {
        super("SpspExporter", EmptyConfig.class);
    }


    private String runSpspFileExporter(Path config, Path samplesetdir, Path outdir, Path workingdir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("/usr/local/bin/Rscript", "/app/R/export_spsp_submission.R",
                "--config", config.toAbsolutePath().toString(),
                "--samplesetdir", samplesetdir.toAbsolutePath().toString(),
                "--outdir", outdir.toAbsolutePath().toString(),
                "--workingdir", workingdir.toAbsolutePath().toString());
        Process SpspFileExporterProcess = pb.start();
        List<String> SpspFileExporterInputLines = new ArrayList<>();
        StreamGobbler SpspFileExporterGobbler = new StreamGobbler(SpspFileExporterProcess.getInputStream(), SpspFileExporterInputLines::add);
        ExecutorService inputStreamReaderService = Executors.newSingleThreadExecutor();
        inputStreamReaderService.submit(SpspFileExporterGobbler);
        boolean exited = SpspFileExporterProcess.waitFor(40, TimeUnit.MINUTES);
        if (!exited) {
            SpspFileExporterProcess.destroyForcibly();
            throw new RuntimeException("SpspFileExporter timed out (after 40 minutes)");
        }
        for (String SpspFileExporterInputLine : SpspFileExporterInputLines) {
            System.out.println(SpspFileExporterInputLine);
        }
        String SpspFileExporterInputReportString = String.join("\n", SpspFileExporterInputLines);
        if (SpspFileExporterProcess.exitValue() != 0) {
            throw new RuntimeException(
                    "SpspFileExporter exited with code " + SpspFileExporterProcess.exitValue() +
                            "\n\n The program reports:\n" + SpspFileExporterInputReportString);
        }
        inputStreamReaderService.shutdown();
        boolean inputStreamReaderTerminated = inputStreamReaderService.awaitTermination(5, TimeUnit.SECONDS);
        if (!inputStreamReaderTerminated) {
            throw new RuntimeException(
                    "The input stream reader did not terminate after executing export_spsp_submission.R!" +
                            "\n\n The program reports:\n" + SpspFileExporterInputReportString);
        }

        return SpspFileExporterInputReportString;
    }

    private String runSpspTransferer() throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("bash", "/app/transfer.sh");
        Process SpspFileTransfererProcess = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                SpspFileTransfererProcess.getInputStream()));
        String SpspFileTransfererInputReportString;
        while ((SpspFileTransfererInputReportString = reader.readLine()) != null) {
            System.out.println("Script output: " + SpspFileTransfererInputReportString);
        }
        boolean exited = SpspFileTransfererProcess.waitFor(20, TimeUnit.MINUTES);
        if (!exited) {
            SpspFileTransfererProcess.destroyForcibly();
            throw new RuntimeException("SpspFileTransferer timed out (after 20 minutes)");
        }
        if (SpspFileTransfererProcess.exitValue() != 0) {
            throw new RuntimeException(
                    "SpspFileTransferer exited with code " + SpspFileTransfererProcess.exitValue() +
                            "\n\n The program reports:\n" + SpspFileTransfererInputReportString);
        }

        return SpspFileTransfererInputReportString;
    }

    private String runSubmissionRecorder(Path config, Path outdir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("/usr/local/bin/Rscript", "/app/R/record_spsp_submission.R",
                "--config", config.toAbsolutePath().toString(),
                "--outdir", outdir.toAbsolutePath().toString());
        Process SubmissionRecorderProcess = pb.start();
        List<String> SubmissionRecorderInputLines = new ArrayList<>();
        StreamGobbler SubmissionRecorderGobbler = new StreamGobbler(SubmissionRecorderProcess.getInputStream(), SubmissionRecorderInputLines::add);
        ExecutorService inputStreamReaderService = Executors.newSingleThreadExecutor();
        inputStreamReaderService.submit(SubmissionRecorderGobbler);
        boolean exited = SubmissionRecorderProcess.waitFor(30, TimeUnit.MINUTES);
        if (!exited) {
            SubmissionRecorderProcess.destroyForcibly();
            throw new RuntimeException("SubmissionRecorder timed out (after 30 minutes)");
        }
        for (String SubmissionRecorderInputLine : SubmissionRecorderInputLines) {
            System.out.println(SubmissionRecorderInputLine);
        }
        String SubmissionRecorderInputReportString = String.join("\n", SubmissionRecorderInputLines);
        if (SubmissionRecorderProcess.exitValue() != 0) {
            throw new RuntimeException(
                    "SubmissionRecorder exited with code " + SubmissionRecorderProcess.exitValue() +
                            "\n\n The program reports:\n" + SubmissionRecorderInputReportString);
        }
        inputStreamReaderService.shutdown();
        boolean inputStreamReaderTerminated = inputStreamReaderService.awaitTermination(5, TimeUnit.SECONDS);
        if (!inputStreamReaderTerminated) {
            throw new RuntimeException(
                    "The input stream reader did not terminate after executing record_spsp_submission.R!" +
                            "\n\n The program reports:\n" + SubmissionRecorderInputReportString);
        }

        return SubmissionRecorderInputReportString;
    }

    @Override
    public void run(String[] args, EmptyConfig _config) throws Exception {
        System.out.println("Parsing harvester configuration file.");
        Path configPath = Path.of("harvester-config.yml");
        // TODO Validate the config file first
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(Files.readString(configPath));

        // Set up email notification system
        List<String> notificationRecipients = (List<String>) config.get("recipients");
        NotificationSystem notificationSystem = new SmtpNotificationSystem(
                (String) config.get("senderSmtpHost"),
                (int) config.get("senderSmtpPort"),
                (String) config.get("senderSmtpUsername"),
                (String) config.get("senderSmtpPassword"),
                (String) config.get("senderAddress"),
                notificationRecipients
        );

        System.out.println("Exporting files for submission to SPSP.");
        try {
            String exportFilesReport = runSpspFileExporter(
                    Path.of("spsp-config.yml"),
                    Path.of(args[0]), // samplesetdir
                    Path.of(args[1]), // outdir
                    Path.of(args[2])  // workingdir
            );
            SimpleReport sendableReport = new SimpleReport(exportFilesReport, "SpspExporter",
                    SendableReport.PriorityLevel.INFO);
            notificationSystem.sendReport(sendableReport);
        } catch (Exception e) {
            notificationSystem.sendReport(new ProgramCrashReport(e, "SpspExporter"));
            e.printStackTrace();
            System.exit(1);
        }

        try {
            System.out.println("Submitting files to SPSP.");
            String submissionReport = runSpspTransferer();

            // Will not do any update unless files are found in <outdir>/sent
            System.out.println("Updating table sequence_identifier to indicate samples uploaded to SPSP.");
            String submissionRecorderReport = runSubmissionRecorder(
                    Path.of("spsp-config.yml"),
                    Path.of(args[1]) // outdir
            );
        } catch (Throwable e) {
            notificationSystem.sendReport(new ProgramCrashReport(e, "SpspExporter"));
            e.printStackTrace();
            System.exit(1);
        }

        // System.out.println("Emailing frameshift deletion diagnostic to SPSP.");
        // TODO
    }
}
