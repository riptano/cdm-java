import com.datastax.driver.core.Cluster;

import java.io.File;

/**
 * Created by jhaddad on 6/29/16.
 */

public class CassandraDatasetManager {

    private static final String YAML_LOCATION = "https://github.com/github/testrepo.git";

    public static void main(String[] args) {
        System.out.println("Hello world!");

        // check for the .cdm directory
        String home_dir = System.getProperty("user.home");
        System.out.println(home_dir);
        File f = new File(home_dir + "/.cdm");
        System.out.println(f);
        f.mkdir();

        // parse the CLI options

        // check for the YAML file

        // create a cluster and session

        Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1").build();


        System.out.println("Finished.");
    }
}
