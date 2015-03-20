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

import static org.sharedhealth.hie.data.Main.PROVIDERS_DATA;
import static org.sharedhealth.hie.data.Main.PROVIDERS_SCRIPTS;

public class PRDataSet {

    public void generate(String inputDir, String outputDirPath) throws Exception {
        System.out.println("Generating SHR-Client Provider scripts. Output directory: " + outputDirPath);
        String providers = String.format("%s/%s", inputDir, PROVIDERS_DATA);
        System.out.println("Picking SHR-Client Provider data from:" + providers);

        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        URL input = new SHRFileUtils().getResource(providers);
        File output = new File(outputDir, PROVIDERS_SCRIPTS);

        CSVParser parser = CSVParser.parse(input, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        List<CSVRecord> csvRecords = parser.getRecords();
        int providerId=0;
        FileUtils.writeStringToFile(output, buildSelectProviderAttributeScript(), Charset.forName("UTF-8"), true);
        for (CSVRecord csvRecord : csvRecords) {
            providerId++;
            FileUtils.writeStringToFile(output, buildInsertProviderScripts(csvRecord, providerId), Charset.forName("UTF-8"), true);
            FileUtils.writeStringToFile(output, buildInsertProviderAttributeScripts(csvRecord, providerId), Charset.forName("UTF-8"), true);
        }

    }

    private String buildSelectProviderAttributeScript() {
     return "SELECT provider_attribute_type_id from provider_attribute_type where name='Organization' into @attribute_type_id;\n";
    }

    public String buildInsertProviderScripts(CSVRecord csvRecord, int providerId) {
        UUID uuid = UUID.randomUUID();
        String providerIdentifier = csvRecord.get("provider_id");
        return String.format("INSERT INTO provider (provider_id, name, identifier, creator, date_created, uuid) SELECT " +
                            "%s,'%s','%s',1,now(),'%s' FROM DUAL WHERE NOT EXISTS(SELECT * FROM provider WHERE identifier = '%s');\n",
                            providerId, StringUtils.replace(csvRecord.get("provider_name"), "'", "''"), providerIdentifier,
                            uuid, providerIdentifier);
    }

    private String buildInsertProviderAttributeScripts(CSVRecord csvRecord, int providerId) {
        UUID uuid = UUID.randomUUID();
        String facilityCode = csvRecord.get("facility_code");
        return String.format("INSERT INTO provider_attribute(provider_id,attribute_type_id, value_reference, uuid, creator, date_created) SELECT " +
                             "%s, @attribute_type_id, '%s', '%s', 1, now() FROM DUAL WHERE NOT EXISTS(select * from provider_attribute WHERE value_reference = '%s');\n",
                            providerId, facilityCode, uuid, facilityCode);
    }



}
