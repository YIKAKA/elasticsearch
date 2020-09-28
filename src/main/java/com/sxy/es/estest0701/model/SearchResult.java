package com.sxy.es.estest0701.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.lucene.search.TotalHits;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
//如果加该注释的字段为NULL，那么就不序列化这个字段了
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchResult {
    private Long totalCount;
    private String[] satellites;
    private String[] sensors;
    private List<data> datas;
}
