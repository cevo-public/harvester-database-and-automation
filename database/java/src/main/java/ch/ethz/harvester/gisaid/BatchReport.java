package ch.ethz.harvester.gisaid;

import java.util.ArrayList;
import java.util.List;


public class BatchReport {

    private int addedEntries;
    private int updatedTotalEntries;
    private int updatedMetadataEntries;
    private int updatedSequenceEntries;
    private int addedEntriesFromUs;
    private int failedEntries;
    private List<WeirdEntryReport> weirdEntryReports = new ArrayList<>();

    public int getAddedEntries() {
        return addedEntries;
    }

    public BatchReport setAddedEntries(int addedEntries) {
        this.addedEntries = addedEntries;
        return this;
    }

    public int getUpdatedTotalEntries() {
        return updatedTotalEntries;
    }

    public BatchReport setUpdatedTotalEntries(int updatedTotalEntries) {
        this.updatedTotalEntries = updatedTotalEntries;
        return this;
    }

    public int getUpdatedMetadataEntries() {
        return updatedMetadataEntries;
    }

    public BatchReport setUpdatedMetadataEntries(int updatedMetadataEntries) {
        this.updatedMetadataEntries = updatedMetadataEntries;
        return this;
    }

    public int getUpdatedSequenceEntries() {
        return updatedSequenceEntries;
    }

    public BatchReport setUpdatedSequenceEntries(int updatedSequenceEntries) {
        this.updatedSequenceEntries = updatedSequenceEntries;
        return this;
    }

    public int getAddedEntriesFromUs() {
        return addedEntriesFromUs;
    }

    public BatchReport setAddedEntriesFromUs(int addedEntriesFromUs) {
        this.addedEntriesFromUs = addedEntriesFromUs;
        return this;
    }

    public int getFailedEntries() {
        return failedEntries;
    }

    public BatchReport setFailedEntries(int failedEntries) {
        this.failedEntries = failedEntries;
        return this;
    }

    public List<WeirdEntryReport> getWeirdEntryReports() {
        return weirdEntryReports;
    }

    public BatchReport setWeirdEntryReports(List<WeirdEntryReport> weirdEntryReports) {
        this.weirdEntryReports = weirdEntryReports;
        return this;
    }



    @Override
    public String toString() {
        return "BatchReport{" +
                "addedEntries=" + addedEntries +
                ", updatedEntries=" + updatedMetadataEntries +
                ", addedEntriesFromUs=" + addedEntriesFromUs +
                ", failedEntries=" + failedEntries +
                ", #weirdReports=" + weirdEntryReports.size() +
                '}';
    }
}
