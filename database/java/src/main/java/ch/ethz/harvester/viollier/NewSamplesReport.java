package ch.ethz.harvester.viollier;

import ch.ethz.harvester.core.ReportAttachment;
import ch.ethz.harvester.core.SendableReportWithAttachments;

import java.util.List;

public class NewSamplesReport implements SendableReportWithAttachments {
    private final List<String> fileNames;
    private final List<String> plates;
    private final int numberSamples;
    private final List<String> involvedSequencingCenters;
    private final List<ReportAttachment> sampleLists;

    public NewSamplesReport(
            List<String> fileNames,
            List<String> plates,
            int numberSamples,
            List<String> involvedSequencingCenters,
            List<ReportAttachment> sampleLists
    ) {
        this.fileNames = fileNames;
        this.plates = plates;
        this.numberSamples = numberSamples;
        this.involvedSequencingCenters = involvedSequencingCenters;
        this.sampleLists = sampleLists;
    }

    @Override
    public PriorityLevel getPriority() {
        return PriorityLevel.INFO;
    }

    @Override
    public String getSubject() {
        return "[Harvester] Received " + numberSamples + " samples on " + plates.size() + " plate(s)";
    }

    @Override
    public String getProgramName() {
        return "ViollierMetadataReceiver";
    }

    @Override
    public String getEmailText() {
        return """
Hi there,

I received new metadata from Viollier! It contains %d plates with %d samples for the sequencing center %s. Please find attached the sample list.

The processed file(s): %s

The plate(s): %s

Best,
Harvester
(On behalf of the ETH covid sequence surveillance team)
""".formatted(
                plates.size(),
                numberSamples,
                String.join(" and ", involvedSequencingCenters),
                String.join(", ", fileNames),
                String.join(", ", plates)
        );
    }

    @Override
    public List<ReportAttachment> getAttachments() {
        return sampleLists;
    }
}
