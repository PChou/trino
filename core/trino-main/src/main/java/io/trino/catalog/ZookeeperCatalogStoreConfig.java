package io.trino.catalog;

import io.airlift.configuration.Config;
import io.airlift.log.Logger;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ZookeeperCatalogStoreConfig
{
    private static final Logger log = Logger.get(ZookeeperCatalogStoreConfig.class);
    private String dynamicEnabled;
    private String zkAddress;
    private String nodePath;
    private String namespace;
    private CuratorFramework curatorFramework;

    @Config("catalog.dynamic.enabled")
    public ZookeeperCatalogStoreConfig setDynamicEnabled(String dynamicEnabled) {
        this.dynamicEnabled = dynamicEnabled;
        return this;
    }

    public boolean getDynamicEnabled() {
        return Boolean.parseBoolean(Objects.requireNonNullElse(dynamicEnabled, "false"));
    }

    @Config("catalog.zk.address")
    public ZookeeperCatalogStoreConfig setCatalogZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
        return this;
    }

    public String getCatalogZkAddress() {
        return Objects.requireNonNullElse(zkAddress, "127.0.0.1:2181");
    }

    @Config("catalog.zk.namespace")
    public ZookeeperCatalogStoreConfig setCatalogZkNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public String getCatalogZkNamespace() {
        return Objects.requireNonNullElse(namespace, "trino");
    }


    @Config("catalog.zk.path")
    public ZookeeperCatalogStoreConfig setCatalogZkPath(String nodePath) {
        this.nodePath = nodePath;
        return this;
    }

    public String getCatalogZkPath() {
        return Objects.requireNonNullElse(nodePath, "/catalog/meta");
    }

    public CuratorFramework getCuratorFramework() {
        createCuratorFramework();
        return curatorFramework;
    }

    private synchronized void createCuratorFramework() {
        if (Objects.isNull(curatorFramework)) {
            requireNonNull(zkAddress, "zookeeper address is null");
            requireNonNull(nodePath, "zookeeper nodePath info is null");
            log.info("get catalog from zookeeper, address: %s, node: %s ", zkAddress, nodePath);
            RetryPolicy backoffRetry = new ExponentialBackoffRetry(3000, 2);
            curatorFramework = CuratorFrameworkFactory.builder()
                    .connectString(zkAddress)
                    .sessionTimeoutMs(30000)
                    .connectionTimeoutMs(30000)
                    .retryPolicy(backoffRetry)
                    .namespace(namespace)
                    .build();
            curatorFramework.start();
        }
    }

}
