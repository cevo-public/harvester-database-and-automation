package ch.ethz.harvester.gisaid;


/**
 * Signals that a GISAID entry is not as expected but it is not severe enough to interrupt the program.
 */
public class WeirdEntryReport {

    private final String gisaidEpiIsl;

    /**
     * The reporting function should have the format [class name]::[function name]. The class can contain the package
     * name but it is not required. Example: "WeirdEntryReport::getReportingFunction" or
     * "ch.ethz.harvester.gisaid.WeirdEntryReport::getReportingFunction".
     */
    private final String reportingFunction;

    private final String message;

    public WeirdEntryReport(String gisaidEpiIsl, String reportingFunction, String message) {
        this.gisaidEpiIsl = gisaidEpiIsl;
        this.reportingFunction = reportingFunction;
        this.message = message;
    }

    public String getGisaidEpiIsl() {
        return gisaidEpiIsl;
    }

    public String getReportingFunction() {
        return reportingFunction;
    }

    public String getMessage() {
        return message;
    }
}
