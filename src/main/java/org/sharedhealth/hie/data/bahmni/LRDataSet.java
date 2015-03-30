package org.sharedhealth.hie.data.bahmni;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.sharedhealth.hie.data.SHRFileUtils;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.sharedhealth.hie.data.Main.LOCATIONS_DATA;
import static org.sharedhealth.hie.data.Main.LOCATIONS_SCRIPTS;

public class LRDataSet {

    public void generate(String inputDir, File outputDir) throws Exception {
        System.out.println(String.format("Generating SHR-Client location scripts. Output directory: %s/%s", outputDir.getPath(), LOCATIONS_SCRIPTS));
        String locations = String.format("%s/%s", inputDir, LOCATIONS_DATA);
        System.out.println("Picking SHR-Client location data from:" + locations);

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
        return String.format("INSERT INTO address_hierarchy_entry (name,level_id,user_generated_id,uuid) SELECT " +
                        "'%s','%s','%s','%s' FROM dual WHERE NOT EXISTS(SELECT * FROM address_hierarchy_entry WHERE user_generated_id = '%s');\n",
                StringUtils.trim(StringUtils.replace(csvRecord.get("name"), "'", "''")),
                csvRecord.get("level"), csvRecord.get("location_code"),
                uuid, csvRecord.get("location_code"));
    }

    public String buildUpdateParentScripts(CSVRecord csvRecord) {

        String parentCode = getParentCode(csvRecord);
        return parentCode != null ?
                String.format("UPDATE address_hierarchy_entry SET parent_id=" +
                        "(SELECT address_hierarchy_entry_id AS parent_id FROM" +
                        "(SELECT address_hierarchy_entry_id FROM address_hierarchy_entry WHERE user_generated_id='%s') AS temp)" +
                        "WHERE user_generated_id='%s';\n", parentCode, csvRecord.get("location_code"))
                : null;

    }

    public String getParentCode(CSVRecord csvRecord) {
        String locationCode = csvRecord.get("location_code");
        String levelCode = csvRecord.get("level_code");

        String parentCode = StringUtils.removeEnd(locationCode, levelCode);

        return isNotBlank(parentCode) ? parentCode : null;
    }


}
