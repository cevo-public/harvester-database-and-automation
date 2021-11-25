package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.general.NucleotideMutationFinder;

import java.time.LocalDate;
import java.util.List;

public class Sequence {

    private ImportMode importMode;
    private boolean metadataChanged = false; // Should only be used together with importMode==UPDATE
    private boolean sequenceChanged = false; // Should only be used together with importMode==UPDATE

    private String gisaidEpiIsl;
    private String strain;
    private String virus;
    private LocalDate date;
    private String dateOriginal;
    private String country;
    private String regionOriginal;
    private String countryOriginal;
    private String division;
    private String location;
    private String host;
    private Integer age;
    private String sex;
    private String pangolinLineage;
    private String gisaidClade;
    private SubmitterInformation submitterInformation;
    private LocalDate dateSubmitted;
    private String samplingStrategy;
    private String seqOriginal;
    private String seqAligned;

    private String nextcladeClade;
    private Float nextcladeQcOverallScore;
    private String nextcladeQcOverallStatus;
    private Integer nextcladeTotalGaps;
    private Integer nextcladeTotalInsertions;
    private Integer nextcladeTotalMissing;
    private Integer nextcladeTotalMutations;
    private Integer nextcladeTotalNonAcgtns;
    private Integer nextcladeTotalPcrPrimerChanges;
    private Integer nextcladeAlignmentStart;
    private Integer nextcladeAlignmentEnd;
    private Integer nextcladeAlignmentScore;
    private Float nextcladeQcMissingDataScore;
    private String nextcladeQcMissingDataStatus;
    private Integer nextcladeQcMissingDataTotal;
    private Float nextcladeQcMixedSitesScore;
    private String nextcladeQcMixedSitesStatus;
    private Integer nextcladeQcMixedSitesTotal;
    private Integer nextcladeQcPrivateMutationsCutoff;
    private Integer nextcladeQcPrivateMutationsExcess;
    private Float nextcladeQcPrivateMutationsScore;
    private String nextcladeQcPrivateMutationsStatus;
    private Integer nextcladeQcPrivateMutationsTotal;
    private String nextcladeQcSnpClustersClustered;
    private Float nextcladeQcSnpClustersScore;
    private String nextcladeQcSnpClustersStatus;
    private Integer nextcladeQcSnpClustersTotal;
    private String nextcladeErrors;

    private List<String> nextcladeMutations;
    private List<NucleotideMutationFinder.Mutation> nucleotideMutations;

    public ImportMode getImportMode() {
        return importMode;
    }

    public Sequence setImportMode(ImportMode importMode) {
        this.importMode = importMode;
        return this;
    }

    public boolean isMetadataChanged() {
        return metadataChanged;
    }

    public Sequence setMetadataChanged(boolean metadataChanged) {
        this.metadataChanged = metadataChanged;
        return this;
    }

    public boolean isSequenceChanged() {
        return sequenceChanged;
    }

    public Sequence setSequenceChanged(boolean sequenceChanged) {
        this.sequenceChanged = sequenceChanged;
        return this;
    }

    public String getGisaidEpiIsl() {
        return gisaidEpiIsl;
    }

    public Sequence setGisaidEpiIsl(String gisaidEpiIsl) {
        this.gisaidEpiIsl = gisaidEpiIsl;
        return this;
    }

    public String getStrain() {
        return strain;
    }

    public Sequence setStrain(String strain) {
        this.strain = strain;
        return this;
    }

    public String getVirus() {
        return virus;
    }

    public Sequence setVirus(String virus) {
        this.virus = virus;
        return this;
    }

    public LocalDate getDate() {
        return date;
    }

    public Sequence setDate(LocalDate date) {
        this.date = date;
        return this;
    }

    public String getDateOriginal() {
        return dateOriginal;
    }

    public Sequence setDateOriginal(String dateOriginal) {
        this.dateOriginal = dateOriginal;
        return this;
    }

    public String getCountry() {
        return country;
    }

    public Sequence setCountry(String country) {
        this.country = country;
        return this;
    }

    public String getRegionOriginal() {
        return regionOriginal;
    }

    public Sequence setRegionOriginal(String regionOriginal) {
        this.regionOriginal = regionOriginal;
        return this;
    }

    public String getCountryOriginal() {
        return countryOriginal;
    }

    public Sequence setCountryOriginal(String countryOriginal) {
        this.countryOriginal = countryOriginal;
        return this;
    }

    public String getDivision() {
        return division;
    }

    public Sequence setDivision(String division) {
        this.division = division;
        return this;
    }

    public String getLocation() {
        return location;
    }

    public Sequence setLocation(String location) {
        this.location = location;
        return this;
    }

    public String getHost() {
        return host;
    }

    public Sequence setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getAge() {
        return age;
    }

    public Sequence setAge(Integer age) {
        this.age = age;
        return this;
    }

    public String getSex() {
        return sex;
    }

    public Sequence setSex(String sex) {
        this.sex = sex;
        return this;
    }

    public String getPangolinLineage() {
        return pangolinLineage;
    }

    public Sequence setPangolinLineage(String pangolinLineage) {
        this.pangolinLineage = pangolinLineage;
        return this;
    }

    public String getGisaidClade() {
        return gisaidClade;
    }

    public Sequence setGisaidClade(String gisaidClade) {
        this.gisaidClade = gisaidClade;
        return this;
    }

    public SubmitterInformation getSubmitterInformation() {
        return submitterInformation;
    }

    public Sequence setSubmitterInformation(SubmitterInformation submitterInformation) {
        this.submitterInformation = submitterInformation;
        return this;
    }

    public LocalDate getDateSubmitted() {
        return dateSubmitted;
    }

    public Sequence setDateSubmitted(LocalDate dateSubmitted) {
        this.dateSubmitted = dateSubmitted;
        return this;
    }

    public String getSamplingStrategy() {
        return samplingStrategy;
    }

    public Sequence setSamplingStrategy(String samplingStrategy) {
        this.samplingStrategy = samplingStrategy;
        return this;
    }

    public String getSeqOriginal() {
        return seqOriginal;
    }

    public Sequence setSeqOriginal(String seqOriginal) {
        this.seqOriginal = seqOriginal;
        return this;
    }

    public String getSeqAligned() {
        return seqAligned;
    }

    public Sequence setSeqAligned(String seqAligned) {
        this.seqAligned = seqAligned;
        return this;
    }

    public String getNextcladeClade() {
        return nextcladeClade;
    }

    public Sequence setNextcladeClade(String nextcladeClade) {
        this.nextcladeClade = nextcladeClade;
        return this;
    }

    public Float getNextcladeQcOverallScore() {
        return nextcladeQcOverallScore;
    }

    public Sequence setNextcladeQcOverallScore(Float nextcladeQcOverallScore) {
        this.nextcladeQcOverallScore = nextcladeQcOverallScore;
        return this;
    }

    public String getNextcladeQcOverallStatus() {
        return nextcladeQcOverallStatus;
    }

    public Sequence setNextcladeQcOverallStatus(String nextcladeQcOverallStatus) {
        this.nextcladeQcOverallStatus = nextcladeQcOverallStatus;
        return this;
    }

    public Integer getNextcladeTotalGaps() {
        return nextcladeTotalGaps;
    }

    public Sequence setNextcladeTotalGaps(Integer nextcladeTotalGaps) {
        this.nextcladeTotalGaps = nextcladeTotalGaps;
        return this;
    }

    public Integer getNextcladeTotalInsertions() {
        return nextcladeTotalInsertions;
    }

    public Sequence setNextcladeTotalInsertions(Integer nextcladeTotalInsertions) {
        this.nextcladeTotalInsertions = nextcladeTotalInsertions;
        return this;
    }

    public Integer getNextcladeTotalMissing() {
        return nextcladeTotalMissing;
    }

    public Sequence setNextcladeTotalMissing(Integer nextcladeTotalMissing) {
        this.nextcladeTotalMissing = nextcladeTotalMissing;
        return this;
    }

    public Integer getNextcladeTotalMutations() {
        return nextcladeTotalMutations;
    }

    public Sequence setNextcladeTotalMutations(Integer nextcladeTotalMutations) {
        this.nextcladeTotalMutations = nextcladeTotalMutations;
        return this;
    }

    public Integer getNextcladeTotalNonAcgtns() {
        return nextcladeTotalNonAcgtns;
    }

    public Sequence setNextcladeTotalNonAcgtns(Integer nextcladeTotalNonAcgtns) {
        this.nextcladeTotalNonAcgtns = nextcladeTotalNonAcgtns;
        return this;
    }

    public Integer getNextcladeTotalPcrPrimerChanges() {
        return nextcladeTotalPcrPrimerChanges;
    }

    public Sequence setNextcladeTotalPcrPrimerChanges(Integer nextcladeTotalPcrPrimerChanges) {
        this.nextcladeTotalPcrPrimerChanges = nextcladeTotalPcrPrimerChanges;
        return this;
    }

    public Integer getNextcladeAlignmentStart() {
        return nextcladeAlignmentStart;
    }

    public Sequence setNextcladeAlignmentStart(Integer nextcladeAlignmentStart) {
        this.nextcladeAlignmentStart = nextcladeAlignmentStart;
        return this;
    }

    public Integer getNextcladeAlignmentEnd() {
        return nextcladeAlignmentEnd;
    }

    public Sequence setNextcladeAlignmentEnd(Integer nextcladeAlignmentEnd) {
        this.nextcladeAlignmentEnd = nextcladeAlignmentEnd;
        return this;
    }

    public Integer getNextcladeAlignmentScore() {
        return nextcladeAlignmentScore;
    }

    public Sequence setNextcladeAlignmentScore(Integer nextcladeAlignmentScore) {
        this.nextcladeAlignmentScore = nextcladeAlignmentScore;
        return this;
    }

    public Float getNextcladeQcMissingDataScore() {
        return nextcladeQcMissingDataScore;
    }

    public Sequence setNextcladeQcMissingDataScore(Float nextcladeQcMissingDataScore) {
        this.nextcladeQcMissingDataScore = nextcladeQcMissingDataScore;
        return this;
    }

    public String getNextcladeQcMissingDataStatus() {
        return nextcladeQcMissingDataStatus;
    }

    public Sequence setNextcladeQcMissingDataStatus(String nextcladeQcMissingDataStatus) {
        this.nextcladeQcMissingDataStatus = nextcladeQcMissingDataStatus;
        return this;
    }

    public Integer getNextcladeQcMissingDataTotal() {
        return nextcladeQcMissingDataTotal;
    }

    public Sequence setNextcladeQcMissingDataTotal(Integer nextcladeQcMissingDataTotal) {
        this.nextcladeQcMissingDataTotal = nextcladeQcMissingDataTotal;
        return this;
    }

    public Float getNextcladeQcMixedSitesScore() {
        return nextcladeQcMixedSitesScore;
    }

    public Sequence setNextcladeQcMixedSitesScore(Float nextcladeQcMixedSitesScore) {
        this.nextcladeQcMixedSitesScore = nextcladeQcMixedSitesScore;
        return this;
    }

    public String getNextcladeQcMixedSitesStatus() {
        return nextcladeQcMixedSitesStatus;
    }

    public Sequence setNextcladeQcMixedSitesStatus(String nextcladeQcMixedSitesStatus) {
        this.nextcladeQcMixedSitesStatus = nextcladeQcMixedSitesStatus;
        return this;
    }

    public Integer getNextcladeQcMixedSitesTotal() {
        return nextcladeQcMixedSitesTotal;
    }

    public Sequence setNextcladeQcMixedSitesTotal(Integer nextcladeQcMixedSitesTotal) {
        this.nextcladeQcMixedSitesTotal = nextcladeQcMixedSitesTotal;
        return this;
    }

    public Integer getNextcladeQcPrivateMutationsCutoff() {
        return nextcladeQcPrivateMutationsCutoff;
    }

    public Sequence setNextcladeQcPrivateMutationsCutoff(Integer nextcladeQcPrivateMutationsCutoff) {
        this.nextcladeQcPrivateMutationsCutoff = nextcladeQcPrivateMutationsCutoff;
        return this;
    }

    public Integer getNextcladeQcPrivateMutationsExcess() {
        return nextcladeQcPrivateMutationsExcess;
    }

    public Sequence setNextcladeQcPrivateMutationsExcess(Integer nextcladeQcPrivateMutationsExcess) {
        this.nextcladeQcPrivateMutationsExcess = nextcladeQcPrivateMutationsExcess;
        return this;
    }

    public Float getNextcladeQcPrivateMutationsScore() {
        return nextcladeQcPrivateMutationsScore;
    }

    public Sequence setNextcladeQcPrivateMutationsScore(Float nextcladeQcPrivateMutationsScore) {
        this.nextcladeQcPrivateMutationsScore = nextcladeQcPrivateMutationsScore;
        return this;
    }

    public String getNextcladeQcPrivateMutationsStatus() {
        return nextcladeQcPrivateMutationsStatus;
    }

    public Sequence setNextcladeQcPrivateMutationsStatus(String nextcladeQcPrivateMutationsStatus) {
        this.nextcladeQcPrivateMutationsStatus = nextcladeQcPrivateMutationsStatus;
        return this;
    }

    public Integer getNextcladeQcPrivateMutationsTotal() {
        return nextcladeQcPrivateMutationsTotal;
    }

    public Sequence setNextcladeQcPrivateMutationsTotal(Integer nextcladeQcPrivateMutationsTotal) {
        this.nextcladeQcPrivateMutationsTotal = nextcladeQcPrivateMutationsTotal;
        return this;
    }

    public String getNextcladeQcSnpClustersClustered() {
        return nextcladeQcSnpClustersClustered;
    }

    public Sequence setNextcladeQcSnpClustersClustered(String nextcladeQcSnpClustersClustered) {
        this.nextcladeQcSnpClustersClustered = nextcladeQcSnpClustersClustered;
        return this;
    }

    public Float getNextcladeQcSnpClustersScore() {
        return nextcladeQcSnpClustersScore;
    }

    public Sequence setNextcladeQcSnpClustersScore(Float nextcladeQcSnpClustersScore) {
        this.nextcladeQcSnpClustersScore = nextcladeQcSnpClustersScore;
        return this;
    }

    public String getNextcladeQcSnpClustersStatus() {
        return nextcladeQcSnpClustersStatus;
    }

    public Sequence setNextcladeQcSnpClustersStatus(String nextcladeQcSnpClustersStatus) {
        this.nextcladeQcSnpClustersStatus = nextcladeQcSnpClustersStatus;
        return this;
    }

    public Integer getNextcladeQcSnpClustersTotal() {
        return nextcladeQcSnpClustersTotal;
    }

    public Sequence setNextcladeQcSnpClustersTotal(Integer nextcladeQcSnpClustersTotal) {
        this.nextcladeQcSnpClustersTotal = nextcladeQcSnpClustersTotal;
        return this;
    }

    public String getNextcladeErrors() {
        return nextcladeErrors;
    }

    public Sequence setNextcladeErrors(String nextcladeErrors) {
        this.nextcladeErrors = nextcladeErrors;
        return this;
    }

    public List<String> getNextcladeMutations() {
        return nextcladeMutations;
    }

    public Sequence setNextcladeMutations(List<String> nextcladeMutations) {
        this.nextcladeMutations = nextcladeMutations;
        return this;
    }

    public List<NucleotideMutationFinder.Mutation> getNucleotideMutations() {
        return nucleotideMutations;
    }

    public void setNucleotideMutations(List<NucleotideMutationFinder.Mutation> nucleotideMutations) {
        this.nucleotideMutations = nucleotideMutations;
    }
}
