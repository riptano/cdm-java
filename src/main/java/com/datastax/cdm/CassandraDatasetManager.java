package com.datastax.cdm;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * Created by jhaddad on 6/29/16.
 */

public class CassandraDatasetManager {

    private static final String YAML_URI = "https://raw.githubusercontent.com/riptano/cdm-java/master/datasets.yaml";
    private final Session session;
    private Map<String, Dataset> datasets;

    CassandraDatasetManager(Map<String, Dataset> datasets, Session session) {
        this.datasets = datasets;
        this.session = session;
    }


    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        System.out.println("Starting CDM");

        // check for the .cdm directory
        String home_dir = System.getProperty("user.home");
        System.out.println(home_dir);
        String cdm_path = home_dir + "/.cdm";

        File f = new File(cdm_path);
        System.out.println(f);

        f.mkdir();

        // check for the YAML file
        File yaml = new File(cdm_path + "/datasets.yaml");
        if (!yaml.exists()) {
            URL y = new URL(YAML_URI);
            FileUtils.copyURLToFile(y, yaml);
        }
        // read in the YAML dataset list
        System.out.println("Loading YAML");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        Map<String, Dataset> data = mapper.readValue(yaml, Map.class);

        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1").build();

        Session session = cluster.newSession();

        // debug: show all datasets no matter what
        CassandraDatasetManager cdm = new CassandraDatasetManager(data, session);
        cdm.list();



        // parse the CLI options
        Options options = new Options();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // create a cluster and session

        // TODO: actually use the parsed options to install the requested dataset
        // for now i'm using the one included with CDM to test
        // load schema using cqlsh - should this use a normal CSV loader eventually?
        if(args.length == 0) {
            cdm.printHelp();
            return;
        }
        
        if(args[0].equals("install")) {
            cdm.install(args[1]);
        } else {
            System.out.println("Not sure what to do.");
        }

        // load data using cqlsh for now

        System.out.println("Finished.");
    }

    void install(String name) throws IOException, InterruptedException {
        // for now i'm using local
        System.out.println("Installing " + name);
        String path = System.getProperty("user.dir");
        System.out.println(path);

        String cdmDir = System.getProperty("user.home") + "/.cdm/";

        // we're dealing with a request to install a remote dataset
        if(!name.equals(".")) {
//            Dataset dataset = datasets.get(name);
            // pull the repo down
            String repoLocation = cdmDir + name;
            System.out.println("Checking for repo at " + repoLocation);
            // if the repo doesn't exist, clone it
            File f = new File(repoLocation);
            if(!f.exists()) {
//                System.out.println("Cloning " + dataset.url);
            }

        }

        // all the paths
        String schema = path + "/schema.cql";
        String dataPath = path + "/data/";
        String configLocation = path + "/cdm.yaml";
        File configFile =  new File(configLocation);

        // load the yaml

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        Config config = mapper.readValue(configFile, Config.class);

        String createKeyspace = "cqlsh -e \"DROP KEYSPACE IF EXISTS " + config.keyspace +
                                "; CREATE KEYSPACE " + config.keyspace +
                                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\"";

        System.out.println(createKeyspace);
        Runtime.getRuntime().exec(new String[]{"bash", "-c", createKeyspace}).waitFor();


        System.out.println("Schema: " + schema);
        String loadSchema = "cqlsh -k " + config.keyspace + " -f " + schema;
        Runtime.getRuntime().exec(new String[]{"bash", "-c", loadSchema}).waitFor();

        System.out.println("Loading data");

        for(String table: config.tables) {
            String dataFile = dataPath + table + ".csv";
            String command = "COPY " + table + " FROM " + "'" + dataFile + "'";
            String loadData= "cqlsh -k " + config.keyspace + " -e \"" + command + "\"";
            System.out.println(loadData);
            Runtime.getRuntime().exec(new String[]{"bash", "-c", loadData}).waitFor();
        }


        System.out.println("Loading data");
    }


    void update() {

    }

    void list() {
        System.out.println("Datasets: ");

        for(Map.Entry<String, Dataset> dataset : datasets.entrySet()) {
            System.out.println(dataset.getKey());
        }

    }
    void printHelp() {
        System.out.println("Put help here.");
    }
}
