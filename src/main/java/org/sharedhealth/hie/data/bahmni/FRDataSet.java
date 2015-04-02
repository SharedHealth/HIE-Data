package org.sharedhealth.hie.data.bahmni;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.sharedhealth.hie.data.Main;
import org.sharedhealth.hie.data.SHRUtils;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import static org.sharedhealth.hie.data.Main.FACILITIES_DATA;
import static org.sharedhealth.hie.data.Main.FACILITIES_SCRIPTS;

public class FRDataSet {

    public void generate(String inputDir, File outputDir) throws Exception {
        System.out.println(String.format("Generating SHR-Client facilty scripts. Output directory: %s/%s",outputDir.getPath(), FACILITIES_SCRIPTS));
        String facilities = String.format("%s/%s", inputDir, FACILITIES_DATA);
        System.out.println("Picking SHR-Client facility data from:" + facilities);

        SHRUtils shrUtils = new SHRUtils();
        URL input = shrUtils.getResource(facilities);
        File output = new File(outputDir, FACILITIES_SCRIPTS);

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        List<CSVRecord> csvRecords = parser.getRecords();

        int facilityId = 0;
        UUID uuid;
        FileUtils.writeStringToFile(output, selectLocationTagTypeScript(), Charset.forName("UTF-8"), true);

        for (CSVRecord csvRecord : csvRecords) {
            facilityId++;
            uuid = UUID.randomUUID();
            FileUtils.writeStringToFile(output, insertFacilityScript(csvRecord, facilityId, uuid), Charset.forName("UTF-8"), true);
            FileUtils.writeStringToFile(output, insertIdMappingScript(csvRecord, uuid), Charset.forName("UTF-8"), true);
            FileUtils.writeStringToFile(output, insertLocationTagMapping(facilityId), Charset.forName("UTF-8"), true);
        }

       shrUtils.updateMarkersForSHRClient(facilityId, "urn://fr/facilities", "facilities/list", output);

    }

    private String selectLocationTagTypeScript() {
        return "SELECT location_tag_id FROM location_tag WHERE name='DGHS Facilities' INTO @tag;\n";
    }

    private String insertLocationTagMapping(int facilityId) {
        return String.format("INSERT INTO location_tag_map SELECT %s,@tag " +
                             "FROM dual WHERE NOT EXISTS(SELECT * FROM location_tag_map WHERE location_id = %s AND location_tag_id = @tag);\n\n", facilityId, facilityId);
    }

    private String insertIdMappingScript(CSVRecord csvRecord, UUID uuid) {
        String facilityCode = csvRecord.get("code");
        return String.format("INSERT INTO shr_id_mapping(internal_id,external_id,type,uri) SELECT '%s','%s','fr_location','%s' " +
                            "FROM DUAL WHERE NOT EXISTS(SELECT * FROM shr_id_mapping WHERE external_id='%s');\n",uuid, facilityCode, getHRMFRSystemUri(facilityCode), facilityCode);
    }

    private String insertFacilityScript(CSVRecord csvRecord, int facilityId, UUID uuid) {
        String facilityName = StringUtils.trim(StringUtils.replace(csvRecord.get("name"), "'", "''"));
        return String.format("INSERT INTO location(location_id,name,creator,date_created,uuid,retired) SELECT %s,'%s',1,now(),'%s', %s " +
                            "FROM DUAL WHERE NOT EXISTS(SELECT * from location where name='%s' and location_id=%s);\n",facilityId, facilityName, uuid, isRetired(csvRecord.get("is_active")), facilityName, facilityId);
    }

    private String isRetired(String active) {
        return "1".equals(active)? "0" : "1";
    }

    private String getHRMFRSystemUri(String facilityCode) {
        return String.format(Main.HRM + "/api/1.0/facilities/%s.json", facilityCode);
    }

}
