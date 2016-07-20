import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by jhaddad on 7/8/16.
 */
public class DatasetList {
    private Map<String, Dataset> data;

    DatasetList(File inputfile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        data = mapper.readValue(inputfile, Map.class);

    }
}
