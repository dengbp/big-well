package com.yr.pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author dengbp
 * @ClassName TableValues
 * @Description TODO
 * @date 2020-05-18 15:04
 */
@Data
public class TableValues implements Serializable {

    private final String tableName;

    private final String valus;

    public TableValues(String tableName, String valus) {
        this.tableName = tableName;
        this.valus = valus;
    }
}
