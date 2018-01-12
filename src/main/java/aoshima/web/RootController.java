package aoshima.web;

import aoshima.model.PrestoQueryResult;
import com.facebook.presto.client.*;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;

@RestController
public class RootController {

    private static final Logger logger = LoggerFactory.getLogger(RootController.class);

    @Value("${presto.coordinator.server}")
    private String prestoCoordinatorServer;

    @Value("${catalog}")
    private String catalog;

    @Value("${schema}")
    private String schema;

    private JettyHttpClient httpClient;

    @Autowired
    private CaffeineCacheManager caffeineCacheManager;

    public RootController() {
        httpClient = new JettyHttpClient(new HttpClientConfig().setConnectTimeout(new Duration(10, TimeUnit.SECONDS)));
    }

    @RequestMapping(value = "/clear_all_cache", method = RequestMethod.DELETE)
    public ResponseEntity<?> clearAllCache() {
        Collection<String> cacheNames = caffeineCacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Cache cache = caffeineCacheManager.getCache(cacheName);
            cache.clear();
        }
        return ResponseEntity.ok(cacheNames);
    }

    @RequestMapping(value = "/list_cache", method = RequestMethod.GET)
    public ResponseEntity<?> listCache() {
        List<String> cacheList = new ArrayList<>();
        Collection<String> cacheNames = caffeineCacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Cache cache = caffeineCacheManager.getCache(cacheName);
            com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = (com.github.benmanes.caffeine.cache.Cache<Object, Object>)cache.getNativeCache();
            Set<Object> objectSet = nativeCache.asMap().keySet();
            for(Object o : objectSet) {
                cacheList.add(o.toString());
            }
        }
        return ResponseEntity.ok(cacheList);
    }

    @RequestMapping(value = "/clear_cache", method = RequestMethod.DELETE)
    public ResponseEntity<?> clearCache(@RequestBody String key) {
        caffeineCacheManager.getCache("query_result").evict(key);
        return ResponseEntity.ok(Arrays.asList(key));
    }

    @RequestMapping(value = "/get_cache", method = RequestMethod.POST)
    public ResponseEntity<?> getCache(@RequestBody String query) {
        Collection<String> cacheNames = caffeineCacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Cache cache = caffeineCacheManager.getCache(cacheName);
            Cache.ValueWrapper valueWrapper = cache.get(query);
            Object result = valueWrapper.get();
            return ResponseEntity.ok(Arrays.asList(result));
        }
        return ResponseEntity.ok(Collections.emptyList());
    }

    @Cacheable(value = "query_result", condition = "#query.equals(\"select table_schema, table_name, column_name, is_nullable, data_type from information_schema.columns\")", keyGenerator = "queryKeyGenerator")
    @RequestMapping(value = "/v1/statement", method = RequestMethod.POST)
    public ResponseEntity<?> getPrestoQueryResult(@RequestBody String query) {
        logger.info("presto query = " + query);

        try (StatementClient client = getStatementClient(query)) {
            while (client.isValid() && (client.current().getData() == null)) {
                client.advance();
            }

            if ((!client.isFailed()) && (!client.isGone()) && (!client.isClosed())) {
                QueryResults results = client.isValid() ? client.current() : client.finalResults();
                String queryId = results.getId();
                if (results.getColumns() == null) {
                    throw new RuntimeException(format("Query %s has no columns\n", results.getId()));
                } else {
                    PrestoQueryResult prestoQueryResult = new PrestoQueryResult();
                    prestoQueryResult.setId(queryId);
                    List<aoshima.model.Column> aoshimaColumns = new ArrayList<>();
                    List<Column> columns = results.getColumns();
                    for(Column column : columns) {
                        aoshima.model.Column aoshimaColumn = new aoshima.model.Column();
                        aoshimaColumn.setName(column.getName());
                        aoshimaColumn.setType(column.getType());
                        aoshimaColumns.add(aoshimaColumn);
                    }
                    prestoQueryResult.setColumns(aoshimaColumns);

                    List<List<Object>> data = new ArrayList<>();
                    while (client.isValid()) {
                        Iterable<List<Object>> rows = client.current().getData();
                        if (rows != null) {
                            for(List<Object> row : rows) {
                                data.add(row);
                            }
                        }
                        client.advance();
                    }

                    prestoQueryResult.setData(data);
                    prestoQueryResult.setInfoUri(results.getInfoUri());
                    prestoQueryResult.setStats(results.getStats());

                    return ResponseEntity.ok(prestoQueryResult);

                }
            }

            if (client.isClosed()) {
                throw new RuntimeException("Query aborted by user");
            } else if (client.isGone()) {
                throw new RuntimeException("Query is gone (server restarted?)");
            } else if (client.isFailed()) {
                QueryResults results = client.finalResults();
                QueryError error = results.getError();
                String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
                throw new RuntimeException(message);
            }

        }
        throw new RuntimeException("should not reach");

    }

    private StatementClient getStatementClient(String query) {


        JsonCodec<QueryResults> jsonCodec = jsonCodec(QueryResults.class);

        ClientSession clientSession = new ClientSession(
                URI.create(prestoCoordinatorServer), "aoshima", "aoshima", null, catalog,
                schema, TimeZone.getDefault().getID(), Locale.getDefault(),
                new HashMap<String, String>(), null, false, new Duration(2, MINUTES));
        return new StatementClient(httpClient, jsonCodec, clientSession, query);
    }

}
