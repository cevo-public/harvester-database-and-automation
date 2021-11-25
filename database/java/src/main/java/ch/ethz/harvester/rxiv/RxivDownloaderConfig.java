package ch.ethz.harvester.rxiv;

import ch.ethz.harvester.core.Config;
import ch.ethz.harvester.core.DatabaseConfig;
import ch.ethz.harvester.core.HttpProxyConfig;
import ch.ethz.harvester.core.LooperConfig;

public class RxivDownloaderConfig implements Config {
    private DatabaseConfig vineyard;
    private HttpProxyConfig httpProxy;
    private LooperConfig looper;

    public DatabaseConfig getVineyard() {
        return vineyard;
    }

    public RxivDownloaderConfig setVineyard(DatabaseConfig vineyard) {
        this.vineyard = vineyard;
        return this;
    }

    public HttpProxyConfig getHttpProxy() {
        return httpProxy;
    }

    public RxivDownloaderConfig setHttpProxy(HttpProxyConfig httpProxy) {
        this.httpProxy = httpProxy;
        return this;
    }

    public LooperConfig getLooper() {
        return looper;
    }

    public void setLooper(LooperConfig looper) {
        this.looper = looper;
    }
}
