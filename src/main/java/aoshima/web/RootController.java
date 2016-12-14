package aoshima.web;

import aoshima.model.PrestoQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
public class RootController {

    private static final Logger logger = LoggerFactory.getLogger(RootController.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CaffeineCacheManager caffeineCacheManager;

    @RequestMapping(value = "/clear_cache", method = RequestMethod.GET)
    public ResponseEntity<?> cacheClear() {

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

        List<String> columns = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();

        int index = 0;
        List<Map<String, Object>> rows = this.jdbcTemplate.queryForList(query);
        for (Map<String, Object> row : rows) {
            List<Object> columnList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (index == 0) {
                    String columnName = entry.getKey();
                    columns.add(columnName);
                }

                Object columnValue = entry.getValue();
                columnList.add(columnValue);
            }
            data.add(columnList);
            index++;
        }

        PrestoQueryResult prestoQueryResult = new PrestoQueryResult();
        prestoQueryResult.setColumns(columns);
        prestoQueryResult.setData(data);

        return ResponseEntity.ok(prestoQueryResult);

    }

}
