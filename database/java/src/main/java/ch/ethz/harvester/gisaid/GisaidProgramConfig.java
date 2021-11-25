package ch.ethz.harvester.gisaid;

import ch.ethz.harvester.core.Config;
import ch.ethz.harvester.core.DatabaseConfig;
import ch.ethz.harvester.core.HttpProxyConfig;
import ch.ethz.harvester.core.NotificationConfig;

public class GisaidProgramConfig implements Config {

    public static class GisaidConfig implements Config {
        private String url;
        private String username;
        private String password;

        public String getUrl() {
            return url;
        }

        public GisaidConfig setUrl(String url) {
            this.url = url;
            return this;
        }

        public String getUsername() {
            return username;
        }

        public GisaidConfig setUsername(String username) {
            this.username = username;
            return this;
        }

        public String getPassword() {
            return password;
        }

        public GisaidConfig setPassword(String password) {
            this.password = password;
            return this;
        }
    }

    public static class GisaidApiImporterConfig implements Config {
        private ImportMode importMode;
        private Boolean updateSubmitterInformation;
        private String workdir;
        private Integer numberWorkers;
        private Integer batchSize;
        private String geoLocationRulesFile;

        public ImportMode getImportMode() {
            return importMode;
        }

        public GisaidApiImporterConfig setImportMode(ImportMode importMode) {
            this.importMode = importMode;
            return this;
        }

        public Boolean getUpdateSubmitterInformation() {
            return updateSubmitterInformation;
        }

        public GisaidApiImporterConfig setUpdateSubmitterInformation(Boolean updateSubmitterInformation) {
            this.updateSubmitterInformation = updateSubmitterInformation;
            return this;
        }

        public String getWorkdir() {
            return workdir;
        }

        public GisaidApiImporterConfig setWorkdir(String workdir) {
            this.workdir = workdir;
            return this;
        }

        public Integer getNumberWorkers() {
            return numberWorkers;
        }

        public GisaidApiImporterConfig setNumberWorkers(Integer numberWorkers) {
            this.numberWorkers = numberWorkers;
            return this;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public GisaidApiImporterConfig setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public String getGeoLocationRulesFile() {
            return geoLocationRulesFile;
        }

        public GisaidApiImporterConfig setGeoLocationRulesFile(String geoLocationRulesFile) {
            this.geoLocationRulesFile = geoLocationRulesFile;
            return this;
        }
    }

    private DatabaseConfig vineyard;
    private NotificationConfig notification;
    private HttpProxyConfig httpProxy;
    private GisaidConfig gisaid;
    private GisaidApiImporterConfig gisaidApiImporter;

    public DatabaseConfig getVineyard() {
        return vineyard;
    }

    public void setVineyard(DatabaseConfig vineyard) {
        this.vineyard = vineyard;
    }

    public NotificationConfig getNotification() {
        return notification;
    }

    public void setNotification(NotificationConfig notification) {
        this.notification = notification;
    }

    public HttpProxyConfig getHttpProxy() {
        return httpProxy;
    }

    public void setHttpProxy(HttpProxyConfig httpProxy) {
        this.httpProxy = httpProxy;
    }

    public GisaidConfig getGisaid() {
        return gisaid;
    }

    public GisaidProgramConfig setGisaid(GisaidConfig gisaid) {
        this.gisaid = gisaid;
        return this;
    }

    public GisaidApiImporterConfig getGisaidApiImporter() {
        return gisaidApiImporter;
    }

    public GisaidProgramConfig setGisaidApiImporter(GisaidApiImporterConfig gisaidApiImporter) {
        this.gisaidApiImporter = gisaidApiImporter;
        return this;
    }
}
