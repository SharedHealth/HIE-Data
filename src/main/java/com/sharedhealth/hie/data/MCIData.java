package com.sharedhealth.hie.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class MCIData {

    public static final String EMPTY_PARENT = "00";

    public void run() throws IOException {
        File mciOutputDir = new File(FileUtils.getTempDirectory(), "mci");
        mciOutputDir.mkdir();
        FileUtils.cleanDirectory(mciOutputDir);

        generateDbScript(mciOutputDir);
        applyMigration(mciOutputDir);
    }

    private void applyMigration(File scriptLocation) {
        new CassandraMigration().migrate(scriptLocation.);
    }

    private File generateDbScript(File mciOutputDir) throws IOException {
        ClassLoader classLoader = this.getClass().getClassLoader();
        File input = new File(classLoader.getResource(Migration.LOCATIONS_DUMP).getFile());
        return generateMCILocationData(input, mciOutputDir);
    }

    private File generateMCILocationData(File input, File mciOutputDir) throws IOException {
        File output = new File(mciOutputDir, "001.cql");
        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        for (CSVRecord csvRecord : parser) {
            FileUtils.writeStringToFile(output, getSql(csvRecord), Charset.forName("UTF-8"), true);
        }
        return output;
    }

    private String getSql(CSVRecord csvRecord) {
        return String.format("INSERT INTO locations (\"code\", \"name\", \"parent\") VALUES " +
                "('%s','%s','%s');\n", csvRecord.get("level_code"), csvRecord.get("name"), getParent(csvRecord));
    }

    private String getParent(CSVRecord csvRecord) {
        String code = csvRecord.get("location_code");
        String parent = StringUtils.substring(code, 0, code.length() - 2);
        return isNotBlank(parent) ? parent : EMPTY_PARENT;
    }
}
