package com.datastax.cdm;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;

/**
 * Created by jhaddad on 9/5/16.
 */
public class CassandraDatasetManagerTest {

    @Test
    public void testCQLStatementGeneration() throws IOException {
        CassandraDatasetManager c = new CassandraDatasetManager();
        Iterable<CSVRecord> records = c.openCSV("data/alltypes.csv");
        CSVRecord r = records.iterator().next();

        HashMap types = new HashMap();
        ArrayList<Field> fieldList = new ArrayList<Field>();
        fieldList.add(new Field("id", "uuid"));
        fieldList.add(new Field("avg", "float"));
        fieldList.add(new Field("cash", "decimal"));
        fieldList.add(new Field("intmap", "map"));
        fieldList.add(new Field("num", "int"));
        fieldList.add(new Field("ts", "timeuuid"));

        String query = c.generateCQL("whatever",
                                     r,
                                     fieldList,
                                     types);
        assertThat(query, containsString("INSERT INTO whatever(id, avg, cash, intmap, num, ts) VALUES"));

    }

}
