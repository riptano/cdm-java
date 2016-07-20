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

    private static final String YAML_URI = "https://raw.githubusercontent.com/riptano/cdm/master/datasets.yaml";
    private final Session session;
    private Map<String, Dataset> datasets;

    CassandraDatasetManager(Map<String, Dataset> datasets, Session session) {
        this.datasets = datasets;
        this.session = session;
    }


    public static void main(String[] args) throws IOException, ParseException {
        System.out.println("Hello world!");

        // check for the .cdm directory
        String home_dir = System.getProperty("user.home");
        System.out.println(home_dir);
        String cdm_path = home_dir + "/.cdm";

        File f = new File(cdm_path);
        System.out.println(f);

        f.mkdir();

        // check for the YAML file
        File yaml = new File(cdm_path + "/cdm.yaml");
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
        cdm.install(".");
        // load data using cqlsh for now

        System.out.println("Finished.");
    }

    void install(String name) throws IOException {
        // for now i'm using local
        String path = System.getProperty("user.dir");
        System.out.println(path);

        // all the paths
        String schema = path + "/schema.cql";
        String dataPath = path + "/data/";
        String config = path + "/cdm.yaml";
        File configFile =  new File(config);

        // load the yaml

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        Config data = mapper.readValue(configFile, Config.class);

        // lets use the test keyspace for now
        // TODO use real keyspace like a champion


        System.out.println("Schema: " + schema);

        System.out.println("Loading schema");
        String command = "cqlsh -f " + schema;

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
}
