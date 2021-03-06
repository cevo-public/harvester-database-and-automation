package ch.ethz.harvester.core;

import java.util.List;

public interface SendAttachmentCapable {

    void sendReportWithAttachment(SendableReportWithAttachments report);

    void sendReportWithAttachment(SendableReportWithAttachments report, List<String> additionalRecipients);

}
