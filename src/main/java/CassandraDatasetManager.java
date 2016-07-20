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
    private Map<String, Dataset> datasets;

    CassandraDatasetManager(Map<String, Dataset> datasets) {
        this.datasets = datasets;
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

        CassandraDatasetManager cass = new CassandraDatasetManager(data);

        // debug: show all datasets no matter what
        System.out.println("Datasets: ");

        for(Map.Entry<String, Dataset> dataset : data.entrySet()) {
            System.out.println(dataset.getKey());
        }

        // parse the CLI options
        Options options = new Options();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // create a cluster and session

        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1").build();

        Session session = cluster.newSession();



        System.out.println("Finished.");
    }

    void install(String name) {

    }
    void update() {

    }

    void list() {

    }
}
