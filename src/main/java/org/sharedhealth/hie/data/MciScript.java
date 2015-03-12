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
import static org.sharedhealth.hie.data.Main.LR_DUMP;

public class MciScript {

    private static final String EMPTY_PARENT = "00";

    public void generate(String outputDirPath) throws Exception {
        generateLocationScripts(outputDirPath);
    }

    private void generateLocationScripts(String outputDirPath) throws Exception {
        System.out.println("Generating MCI location scripts. Output directory: " + outputDirPath);
        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        URL input = getResource(LR_DUMP);
        File output = new File(outputDir, "mci-locations.cql");

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        for (CSVRecord csvRecord : parser) {
            FileUtils.writeStringToFile(output, buildScripts(csvRecord), Charset.forName("UTF-8"), true);
        }
    }

    private String buildScripts(CSVRecord csvRecord) {
        return String.format("INSERT INTO mci.locations (\"code\", \"name\", \"parent\") VALUES " +
                "('%s','%s','%s') IF NOT EXISTS;\n", csvRecord.get("level_code"), csvRecord.get("name"), getParent(csvRecord));
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
