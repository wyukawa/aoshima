package aoshima.model;

import lombok.Data;

import java.util.List;

@Data
public class PrestoQueryResult {

    private List<Column> columns;

    private List<List<Object>> data;
}


