package aoshima.web;

import aoshima.model.*;
import com.facebook.presto.client.*;
import com.facebook.presto.client.Column;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @RequestMapping(value = "/clear_cache", method = RequestMethod.GET)
    public ResponseEntity<?> clearCache() {

        Collection<String> cacheNames = caffeineCacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Cache cache = caffeineCacheManager.getCache(cacheName);
            cache.clear();
        }

        return ResponseEntity.ok(cacheNames);
    }

    @Cacheable("query_result")
    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public ResponseEntity<?> getPrestoQueryResult(@RequestParam("query") String query) {
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
                URI.create(prestoCoordinatorServer), "aoshima", "aoshima", catalog,
                schema, TimeZone.getDefault().getID(), Locale.getDefault(),
                new HashMap<String, String>(), null, false, new Duration(2, MINUTES));
        return new StatementClient(httpClient, jsonCodec, clientSession, query);
    }

}
