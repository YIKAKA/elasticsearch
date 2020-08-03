package com.sxy.es.estest0701.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Result<T> implements Serializable {
    private String status = "ok";
    private String error;
    private T result;

    public Result<T> status(String status) {
        this.status = status;
        return this;
    }

    public Result<T> msg(String msg) {
        this.error = msg;
        return this;
    }

    public Result<T> result(T result) {
        this.result = result;
        return this;
    }
}
