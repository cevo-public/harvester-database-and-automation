package ch.ethz.harvester.pangolineage;

import java.util.Objects;

class PangolinLineageAlias {
    private final String alias;
    private final String fullName;

    public PangolinLineageAlias(String alias, String fullName) {
        this.alias = alias;
        this.fullName = fullName;
    }

    public String getAlias() {
        return alias;
    }

    public String getFullName() {
        return fullName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PangolinLineageAlias that = (PangolinLineageAlias) o;
        return Objects.equals(alias, that.alias) && Objects.equals(fullName, that.fullName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, fullName);
    }
}
