package aoshima.model;

import lombok.Data;

import java.util.List;

@Data
public class PrestoQueryResult {

    private List<String> columns;

    private List<List<Object>> data;
}


