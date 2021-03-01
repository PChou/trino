package io.trino.catalog;

import io.airlift.log.Logger;
import io.trino.server.security.ResourceSecurity;
import org.apache.zookeeper.CreateMode;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;

@Path("/v1/catalog")
public class ZookeeperCatalogResource
{
    private static final Logger log = Logger.get(ZookeeperCatalogResource.class);

    private final ZookeeperCatalogStoreConfig config;
    private final String catalogZkPath;
    private final boolean enabledDynamic;

    @Inject
    public ZookeeperCatalogResource(ZookeeperCatalogStoreConfig config) {
        this.config = config;
        this.catalogZkPath = config.getCatalogZkPath();
        this.enabledDynamic = config.getDynamicEnabled();
    }

    private void check() {
        if (!enabledDynamic) {
            throw ZookeeperCatalogException.newInstance("please set catalog.dynamic.enabled=true in node.properties");
        }
    }

    @GET
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<CatalogInfo> list() {
        check();
        try {
            List<String> list = config.getCuratorFramework().getChildren().forPath(catalogZkPath);
            return list.stream()
                    .map(s -> {
                        try {
                            byte[] bytes = config.getCuratorFramework().getData().forPath(catalogZkPath + "/" + s);
                            return JsonUtil.toObj(new String(bytes, StandardCharsets.UTF_8), CatalogInfo.class);
                        } catch (Exception e) {
                            log.error("get catalog [%s] error", s);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("failed get catalog list", e);
            return Collections.emptyList();
        }
    }

    @GET
    @Path("/{catalogName}")
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public CatalogInfo detail(@PathParam("catalogName") String catalogName) {
        check();
        try {
            byte[] bytes = config.getCuratorFramework().getData().forPath(catalogZkPath + "/" + catalogName);
            return JsonUtil.toObj(new String(bytes, StandardCharsets.UTF_8), CatalogInfo.class);
        } catch (Exception e) {
            log.error("failed get catalog detail");
            throw ZookeeperCatalogException.newInstance("failed get catalog detail", e);
        }
    }

    /**
     * 保存catalog
     * 新增或更新
     *
     * @param catalogInfo CatalogInfo
     */
    @POST
    @ResourceSecurity(PUBLIC)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void save(CatalogInfo catalogInfo) {
        check();
        try {
            requireNonNull(catalogInfo.getCatalogName(), "catalog can not be null");
            requireNonNull(catalogInfo.getConnectorName(), "connectorName can not be null");
            requireNonNull(catalogInfo.getProperties(), "properties can not be null");
            if (catalogInfo.getProperties().isEmpty()) {
                throw ZookeeperCatalogException.newInstance("catalog properties is null");
            }
            config.getCuratorFramework().create().orSetData().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(catalogZkPath + "/" + catalogInfo.getCatalogName(),
                            JsonUtil.toJson(catalogInfo).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw ZookeeperCatalogException.newInstance(Objects.requireNonNullElse(e.getCause(), e).getMessage(), e);
        }
    }

    @DELETE
    @Path("{catalogName}")
    @ResourceSecurity(PUBLIC)
    public void delete(@PathParam("catalogName") String catalogName) {
        check();
        try {
            config.getCuratorFramework().delete().guaranteed().deletingChildrenIfNeeded()
                    .withVersion(-1)
                    .forPath(catalogZkPath + "/" + catalogName);
        } catch (Exception e) {
            throw ZookeeperCatalogException.newInstance(Objects.requireNonNullElse(e.getCause(), e).getMessage(), e);
        }
    }

}
