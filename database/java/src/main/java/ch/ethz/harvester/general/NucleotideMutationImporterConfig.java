package ch.ethz.harvester.general;

import ch.ethz.harvester.core.Config;
import ch.ethz.harvester.core.DatabaseConfig;

public class NucleotideMutationImporterConfig implements Config {
    private DatabaseConfig vineyard;

    public DatabaseConfig getVineyard() {
        return vineyard;
    }

    public void setVineyard(DatabaseConfig vineyard) {
        this.vineyard = vineyard;
    }
}
