package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.core.SendableReport;

import java.util.Set;


public class UnexpectedDataReport implements SendableReport {

    private final Set<String> missingFields;
    private final Set<String> missingRequiredFields;
    private final Set<String> additionalFields;

    public UnexpectedDataReport(
            Set<String> missingFields,
            Set<String> missingRequiredFields,
            Set<String> additionalFields
    ) {
        this.missingFields = missingFields;
        this.missingRequiredFields = missingRequiredFields;
        this.additionalFields = additionalFields;
    }

    public Set<String> getMissingFields() {
        return missingFields;
    }

    public Set<String> getMissingRequiredFields() {
        return missingRequiredFields;
    }

    public Set<String> getAdditionalFields() {
        return additionalFields;
    }

    @Override
    public PriorityLevel getPriority() {
        if (!missingRequiredFields.isEmpty()) {
            return PriorityLevel.FATAL;
        } else {
            return PriorityLevel.WARNING;
        }
    }

    @Override
    public String getProgramName() {
        return "GisaidApiImporter";
    }

    @Override
    public String getEmailText() {
        String text = "The data package from GISAID was not as expected.\n\n";
        if (!missingRequiredFields.isEmpty()) {
            text += "The following required fields were missing: " +
                    String.join(", ", missingRequiredFields) + "\n";
        }
        if (!missingFields.isEmpty()) {
            text += "The following fields were missing: " +
                    String.join(", ", missingFields) + "\n";
        }
        if (!additionalFields.isEmpty()) {
            text += "The following unexpected fields were found: " +
                    String.join(", ", additionalFields) + "\n";
        }
        return text;
    }
}
