package com.sxy.es.estest0701.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
//如果加该注释的字段为NULL，那么就不序列化这个字段了
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchResult {
    private Long totalCount;
    private List<String> satellites;
    private List<String> sensors;
    private List<data> datas;
    private String objects;
}
