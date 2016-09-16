package com.datastax.cdm;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jhaddad on 9/14/16.
 */
public class CDMArgs {

    @Parameter
    public List<String> command = new ArrayList<>();

    @Parameter(names = {"--host", "-h"}, description = "Hostname of node in cluster")
    public String host = "localhost";

    @Parameter(names = "--rf", description = "Replication Factor")
    public Integer rf = 1;

    @Parameter(names = {"--no-data", "--nodata"}, description = "Only set up schema")
    public Boolean noData = false; // setting this is schema only

}
