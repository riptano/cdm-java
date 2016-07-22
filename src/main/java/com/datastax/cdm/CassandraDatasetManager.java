package com.datastax.cdm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * Created by jhaddad on 6/29/16.
 */

public class CassandraDatasetManager {

    private static final String YAML_URI = "https://raw.githubusercontent.com/riptano/cdm-java/master/datasets.yaml";
    private Map<String, Dataset> datasets;

    CassandraDatasetManager(Map<String, Dataset> datasets) {
        this.datasets = datasets;
    }


    public static void main(String[] args) throws IOException, ParseException, InterruptedException, GitAPIException {
        System.out.println("Starting CDM");

        // check for the .cdm directory
        String home_dir = System.getProperty("user.home");
//        System.out.println(home_dir);
        String cdm_path = home_dir + "/.cdm";

        File f = new File(cdm_path);
//        System.out.println(f);

        f.mkdir();

        // check for the YAML file
        File yaml = new File(cdm_path + "/datasets.yaml");
        if (!yaml.exists()) {
            URL y = new URL(YAML_URI);
            FileUtils.copyURLToFile(y, yaml);
        }
        // read in the YAML dataset list
//        System.out.println("Loading Configuration YAML");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        // why extra work? Java Type Erasure will prevent type detection otherwise
        Map<String, Dataset> data = mapper.readValue(yaml, new TypeReference<Map<String, Dataset>>() {} );

        // debug: show all datasets no matter what
        CassandraDatasetManager cdm = new CassandraDatasetManager(data);


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
        } else if (args[0].equals("list")) {
            cdm.list();
        }
        else {
            System.out.println("Not sure what to do.");
        }

        // load data using cqlsh for now

        System.out.println("Finished.");
    }

    void install(String name) throws IOException, InterruptedException, GitAPIException {
        // for now i'm using local
        System.out.println("Installing " + name);


        String cdmDir = System.getProperty("user.home") + "/.cdm/";

        String schema;
        String dataPath;
        String configLocation;
        String path;

        // temp variables that need to change depending on the dataset

        // we're dealing with a request to install a local dataset
        if(name.equals(".")) {
            path = System.getProperty("user.dir");
            // do nothing here
        } else {
            Dataset dataset = datasets.get(name);
            // pull the repo down
            String repoLocation = cdmDir + name;
            path = repoLocation;

            System.out.println("Checking for repo at " + repoLocation);
            // if the repo doesn't exist, clone it
            File f = new File(repoLocation);

            if(!f.exists()) {
                System.out.println("Cloning " + dataset.url);
                f.mkdir();

                try {
                    Git result = Git.cloneRepository()
                            .setURI(dataset.url)
                            .setDirectory(f)
                            .call();
                    System.out.println("Having repository: " + result.getRepository().getDirectory());
                    // Note: the call() returns an opened repository
                    // already which needs to be closed to avoid file handle leaks!
                }
                 catch (InvalidRemoteException e) {
                    e.printStackTrace();
                } catch (TransportException e) {
                    e.printStackTrace();
                } catch (GitAPIException e) {
                    e.printStackTrace();
                }

            } else {
                // pull the latest
                System.out.println("Pulling latest");
                Git repo = Git.open(f);
                repo.pull().call();
            }
        }
        System.out.println("CDM is using dataset path: " + path);

        // all the paths
        dataPath = path + "/data/";
        configLocation = path + "/cdm.yaml";
        schema = path + "/schema.cql";

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
