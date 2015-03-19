package org.sharedhealth.hie.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.sharedhealth.hie.data.Main.LOCATIONS_DATA;
import static org.sharedhealth.hie.data.Main.LOCATIONS_SCRIPTS;

public class BDSHRClientScript {

    public void generate(String inputDir, String outputDirPath) throws Exception {
        generateLocationScripts(inputDir, outputDirPath);
    }

    private void generateLocationScripts(String inputDir, String outputDirPath) throws Exception {
        System.out.println("Generating SHR-Client location scripts. Output directory: " + outputDirPath);
        String locations = String.format("%s/%s", inputDir, LOCATIONS_DATA);
        System.out.println("Picking SHR-Client location data from:" + locations);

        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        URL input = new SHRFileUtils().getResource(locations);
        File output = new File(outputDir, LOCATIONS_SCRIPTS);

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        List<CSVRecord> csvRecords = parser.getRecords();
        for (CSVRecord csvRecord : csvRecords) {
            FileUtils.writeStringToFile(output, buildInsertScripts(csvRecord), Charset.forName("UTF-8"), true);
        }
        for (CSVRecord csvRecord : csvRecords) {
            String updateParentScript = buildUpdateParentScripts(csvRecord);
            if (updateParentScript != null) {
                FileUtils.writeStringToFile(output, updateParentScript, Charset.forName("UTF-8"), true);
            }
        }
    }

    public String buildInsertScripts(CSVRecord csvRecord) {
        UUID uuid = UUID.randomUUID();
        return String.format("INSERT INTO address_hierarchy_entry (name,level_id,user_generated_id,uuid) select " +
                        "'%s','%s','%s','%s' from dual where not exists(SELECT * from address_hierarchy_entry where user_generated_id = '%s');\n",
                StringUtils.replace(csvRecord.get("name"), "'", "''"),
                csvRecord.get("level"), csvRecord.get("location_code"),
                uuid, csvRecord.get("location_code"));
    }

    public String buildUpdateParentScripts(CSVRecord csvRecord) {

        String parentCode = getParentCode(csvRecord);
        return parentCode != null ?
                String.format("update address_hierarchy_entry set parent_id=" +
                        "(select address_hierarchy_entry_id as parent_id from" +
                        "(select address_hierarchy_entry_id from address_hierarchy_entry where user_generated_id='%s') as temp)" +
                        "where user_generated_id='%s';\n", parentCode, csvRecord.get("location_code"))
                : null;

    }

    public String getParentCode(CSVRecord csvRecord) {
        String locationCode = csvRecord.get("location_code");
        String levelCode = csvRecord.get("level_code");

        String parentCode = StringUtils.removeEnd(locationCode, levelCode);

        return isNotBlank(parentCode) ? parentCode : null;
    }


}
