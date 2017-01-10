package aoshima.model;

import com.facebook.presto.client.StatementStats;
import lombok.Data;

import java.net.URI;
import java.util.List;

@Data
public class PrestoQueryResult {

    private String id;

    private URI infoUri;

    private List<Column> columns;

    private List<List<Object>> data;

    private StatementStats stats;
}


