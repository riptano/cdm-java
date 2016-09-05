package com.datastax.cdm;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;

import javax.management.Query;
import java.lang.StringBuilder;

//import com.datastax.loader.CqlDelimLoadTask;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Created by jhaddad on 6/29/16.
 */

public class CassandraDatasetManager {

    private static final String YAML_URI = "https://raw.githubusercontent.com/riptano/cdm-java/master/datasets.yaml";
    private Map<String, Dataset> datasets;
    private Session session;

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

        // connect to the cluster via the driver

        if(args[0].equals("install")) {
            cdm.install(args[1]);
        } else if (args[0].equals("list")) {
            cdm.list();
        } else if (args[0].equals("new")) {
            cdm.new_dataset(args[1]);
        } else if (args[0].equals("dump")) {
            cdm.dump();
        } else if (args[0].equals("update")) {
            cdm.update();
        } else {
            System.out.println("Not sure what to do.");
        }

        // load data using cqlsh for now

        System.out.println("Finished.");
    }

    private void dump() throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Config config = mapper.readValue(new File("cdm.yaml"), Config.class);

        for(String table: config.tables) {
            StringBuilder command = new StringBuilder();
            command.append("cqlsh -k ")
                   .append(config.keyspace)
                   .append(" -e \"")
                   .append("COPY ")
                   .append(table)
                   .append(" TO 'data/")
                   .append(table)
                   .append(".csv'\"");
            System.out.println(command);
            Runtime.getRuntime().exec(new String[]{"bash", "-c", command.toString()}).waitFor();
        }

    }

    private void new_dataset(String arg) throws FileNotFoundException, UnsupportedEncodingException {
        System.out.println("Creating new dataset " + arg);
        File f = new File(arg);
        f.mkdir();
        String conf = arg + "/" + "cdm.yaml";
        PrintWriter config = new PrintWriter(conf, "UTF-8");
        String sample_conf = "keyspace: " + arg + "\n" +
                "tables:\n" +
                "    - tablename\n" +
                "version: 2.1";
        config.println(sample_conf);
        config.close();
        File data_dir = new File(arg + "/data");
        data_dir.mkdir();
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

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();

//        String createKeyspace = "DROP KEYSPACE IF EXISTS " + config.keyspace +
//                                "; CREATE KEYSPACE " + config.keyspace +
//                                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";

        StringBuilder createKeyspace = new StringBuilder();
        createKeyspace.append(" CREATE KEYSPACE " )
                      .append(config.keyspace)
                      .append( " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        System.out.println(createKeyspace);
        session.execute("DROP KEYSPACE IF EXISTS " + config.keyspace);
        session.execute(createKeyspace.toString());

        System.out.println("Schema: " + schema);
        String loadSchema = "cqlsh -k " + config.keyspace + " -f " + schema;
        Runtime.getRuntime().exec(new String[]{"bash", "-c", loadSchema}).waitFor();

        System.out.println("Loading data");

        Cluster cluster2 = Cluster.builder()
                           .addContactPoint("127.0.0.1")
                           .build();

        for(String table: config.tables) {
            String dataFile = dataPath + table + ".csv";

            Reader in = new FileReader(dataFile);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
            System.out.println("Importing " + table);
            KeyspaceMetadata keyspaceMetadata = cluster2.getMetadata()
                                                        .getKeyspace(config.keyspace);
            TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
//            PreparedStatement p = session.prepare();
            List<ColumnMetadata> columns = tableMetadata.getColumns();

            for(CSVRecord record: records) {
                // generate a CQL statement
                Insert insert = QueryBuilder.insertInto(tableMetadata);

                int i = 0;
                for(ColumnMetadata cm: columns) {
                    String t = cm.getType().getName().toString().toLowerCase();
                    System.out.println(t);
                    if(t.equals("int")) {
                        insert.value(cm.getName(), new Integer(record.get(i)));
                    }
                    else if(t.equals("float")) {
                        insert.value(cm.getName(), new Float(record.get(i)));
                    }
                    else {
                        insert.value(cm.getName(), record.get(i));
                    }
                    i++;
                }
                String query = insert.toString();
                System.out.println(query);
                session.execute(query);
            }
        }


        System.out.println("Loading data");
    }


    void update() throws IOException {
        System.out.println("Updating datasets...");
        String home_dir = System.getProperty("user.home");
        String cdm_path = home_dir + "/.cdm";

        File yaml = new File(cdm_path + "/datasets.yaml");
        URL y = new URL(YAML_URI);
        FileUtils.copyURLToFile(y, yaml);
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
