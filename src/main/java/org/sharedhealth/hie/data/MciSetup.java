package org.sharedhealth.hie.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.sharedhealth.hie.data.Main.LOCATIONS_DUMP;

public class MciSetup {

    private static final String EMPTY_PARENT = "00";
    private static final String SRC_MAIN_RESOURCES = "src/main/resources";
    private static final String SCRIPTS_MCI_LOCATION = "scripts/mci/location";

    public void generateScripts() throws IOException {
        generateLocationScripts();
    }

    private void generateLocationScripts() throws IOException {
        System.out.println("Generating MCI location scripts.");
        File outputDir = new File(SRC_MAIN_RESOURCES, "scripts/mci/location");
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        File input = getResource(LOCATIONS_DUMP);
        File output = new File(outputDir, "001.cql");
        FileUtils.writeStringToFile(output, "TRUNCATE locations;\n");

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        for (CSVRecord csvRecord : parser) {
            FileUtils.writeStringToFile(output, buildScripts(csvRecord), Charset.forName("UTF-8"), true);
        }
    }

    private String buildScripts(CSVRecord csvRecord) {
        return String.format("INSERT INTO locations (\"code\", \"name\", \"parent\") VALUES " +
                "('%s','%s','%s');\n", csvRecord.get("level_code"), csvRecord.get("name"), getParent(csvRecord));
    }

    private String getParent(CSVRecord csvRecord) {
        String code = csvRecord.get("location_code");
        String parent = StringUtils.substring(code, 0, code.length() - 2);
        return isNotBlank(parent) ? parent : EMPTY_PARENT;
    }

    public void applyScripts() throws IOException {
        applyLocationScripts();
    }

    private void applyLocationScripts() throws IOException {
        System.out.println("Applying MCI location scripts.");
        new CassandraSetup().applyScripts(SCRIPTS_MCI_LOCATION, getCassandraProperties());
    }

    private Properties getCassandraProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(getResource("config/mci/cassandra.properties")));
        return properties;
    }

    private File getResource(String resource) {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return new File(classLoader.getResource(resource).getFile());
    }
}
