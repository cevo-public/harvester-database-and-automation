package ch.ethz.harvester.core;

import java.util.List;

public interface SendableReportWithAttachments extends SendableReport {

    List<ReportAttachment> getAttachments();

}
