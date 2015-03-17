package org.sharedhealth.hie.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.sharedhealth.hie.data.Main.LOCATIONS_DATA;
import static org.sharedhealth.hie.data.Main.LOCATIONS_SCRIPTS;

public class MciScript {

    private static final String EMPTY_PARENT = "00";

    public void generate(String inputDir, String outputDirPath) throws Exception {
        generateLocationScripts(inputDir, outputDirPath);
    }

    private void generateLocationScripts(String inputDir, String outputDirPath) throws Exception {
        System.out.println("Generating MCI location scripts. Output directory: " + outputDirPath);
        String locations = String.format("%s/%s", inputDir, LOCATIONS_DATA);
        System.out.println("Picking MCI location data from:" + locations);

        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        URL input = getResource(locations);
        File output = new File(outputDir, LOCATIONS_SCRIPTS);

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        for (CSVRecord csvRecord : parser) {
            FileUtils.writeStringToFile(output, buildScripts(csvRecord), Charset.forName("UTF-8"), true);
        }
    }

    public String buildScripts(CSVRecord csvRecord) {
        return String.format("INSERT INTO locations (\"code\", \"name\", \"active\",\"parent\") VALUES " +
                "('%s','%s','%s','%s') IF NOT EXISTS;\n", csvRecord.get("level_code"), StringUtils.replace(csvRecord.get("name"), "'", "''") ,"1", getParent(csvRecord));

    }

    private String getParent(CSVRecord csvRecord) {
        String code = csvRecord.get("location_code");
        String parent = StringUtils.substring(code, 0, code.length() - 2);
        return isNotBlank(parent) ? parent : EMPTY_PARENT;
    }

    private URL getResource(String resource) throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return classLoader.getResource(resource).toURI().toURL();
    }
}
