package ch.ethz.harvester.viollier;

import java.util.Set;

class AutomationState {
    private Set<String> processedFiles;
    private Set<String> filesInProcessing;

    public Set<String> getProcessedFiles() {
        return processedFiles;
    }

    public AutomationState setProcessedFiles(Set<String> processedFiles) {
        this.processedFiles = processedFiles;
        return this;
    }

    public Set<String> getFilesInProcessing() {
        return filesInProcessing;
    }

    public AutomationState setFilesInProcessing(Set<String> filesInProcessing) {
        this.filesInProcessing = filesInProcessing;
        return this;
    }
}
