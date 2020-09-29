package com.sxy.es.estest0701.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PropertiesRequest implements Serializable {
  private String key;
  private String value;
  private String valueType;

  @Override
  public String toString() {
    return "PropertiesRequest{" +
      "key='" + key + '\'' +
      ", value='" + value + '\'' +
      ", valueType='" + valueType + '\'' +
      '}';
  }
}
