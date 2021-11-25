package ch.ethz.harvester.pangolineage;

import ch.ethz.harvester.core.SendableReport;

import java.util.List;
import java.util.stream.Collectors;

class ImportReport implements SendableReport {
    private final List<PangolinLineageAlias> toAdd;
    private final List<PangolinLineageAlias> toDelete;
    private final List<PangolinLineageAlias> toUpdate;
    private final List<PangolinLineageAlias> unexpected;

    public ImportReport(
            List<PangolinLineageAlias> toAdd,
            List<PangolinLineageAlias> toDelete,
            List<PangolinLineageAlias> toUpdate,
            List<PangolinLineageAlias> unexpected
    ) {
        this.toAdd = toAdd;
        this.toDelete = toDelete;
        this.toUpdate = toUpdate;
        this.unexpected = unexpected;
    }

    @Override
    public PriorityLevel getPriority() {
        if (!unexpected.isEmpty()) {
            return PriorityLevel.FATAL;
        } else if (!toUpdate.isEmpty()) {
            return PriorityLevel.WARNING;
        }
        return PriorityLevel.INFO;
    }

    @Override
    public String getProgramName() {
        return "PangolinLineageAliasImporter";
    }

    @Override
    public String getEmailText() {
        return "I found changes in the pangolin lineage aliases.\n\n"
                + (unexpected.isEmpty() ? "No unexpected entries were found." : "FATAL: There were unexpected entries.") + "\n"
                + (toUpdate.isEmpty() ? "Existing entries were not changed." : "WARN: Existing entries were changed.") + "\n"
                + (unexpected.isEmpty() ? "The following changes will be written to the database:"
                : "The following changes would be written to the database if we didn't have the unexpected errors:") + "\n\n"
                + "Adding: " + toAdd.stream().map(a -> a.getAlias() + "=" + a.getFullName()).collect(Collectors.joining(", ")) + "\n"
                + "Deleting: " + toDelete.stream().map(a -> a.getAlias() + "=" + a.getFullName()).collect(Collectors.joining(", ")) + "\n"
                + "Updating - the new values: " + toUpdate.stream().map(a -> a.getAlias() + "=" + a.getFullName()).collect(Collectors.joining(", ")) + "\n"
                + "Unexpected entries: " + unexpected.stream().map(a -> a.getAlias() + "=" + a.getFullName()).collect(Collectors.joining(", ")) + "\n";
    }
}
