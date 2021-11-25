package ch.ethz.harvester.pangolineage;

import ch.ethz.harvester.core.*;

public class PangolinLineageAliasImporterConfig implements Config {
    private NotificationConfig notification;
    private HttpProxyConfig httpProxy;
    private DatabaseConfig vineyard;
    private LooperConfig looper;

    public NotificationConfig getNotification() {
        return notification;
    }

    public PangolinLineageAliasImporterConfig setNotification(NotificationConfig notification) {
        this.notification = notification;
        return this;
    }

    public HttpProxyConfig getHttpProxy() {
        return httpProxy;
    }

    public PangolinLineageAliasImporterConfig setHttpProxy(HttpProxyConfig httpProxy) {
        this.httpProxy = httpProxy;
        return this;
    }

    public DatabaseConfig getVineyard() {
        return vineyard;
    }

    public PangolinLineageAliasImporterConfig setVineyard(DatabaseConfig vineyard) {
        this.vineyard = vineyard;
        return this;
    }

    public LooperConfig getLooper() {
        return looper;
    }

    public void setLooper(LooperConfig looper) {
        this.looper = looper;
    }
}
