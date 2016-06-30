import com.datastax.driver.core.Cluster;
import org.eclipse.jgit.api.Git;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by jhaddad on 6/29/16.
 */

public class CassandraDatasetManager {

    private static final String YAML_URI = "https://raw.githubusercontent.com/riptano/cdm/master/datasets.yaml";

    public static void main(String[] args) throws IOException {
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

        // parse the CLI options


        // create a cluster and session

        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1").build();


        System.out.println("Finished.");
    }
}
