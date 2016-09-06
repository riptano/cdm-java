package com.datastax.cdm;

/**
 * Created by jhaddad on 9/5/16.
 */
public class Field {
    public String name;
    public String type;

    Field(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
