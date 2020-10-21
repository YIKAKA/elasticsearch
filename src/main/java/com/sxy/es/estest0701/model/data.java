package com.sxy.es.estest0701.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class data implements Serializable {
  private String boundary;
  private String satellite;
  private String sensor;
  private String cloud;
  private String time;
  private String resolution;
  private Object otherProperties;
  private String imageID;

}
