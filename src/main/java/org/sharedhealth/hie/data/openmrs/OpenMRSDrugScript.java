package org.sharedhealth.hie.data.openmrs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.sharedhealth.hie.data.SHRUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import static org.sharedhealth.hie.data.SHRUtils.writeLineToFile;

public class OpenMRSDrugScript {
    public static final String OPENMRS_DRUGS_SCRIPT = "openmrs_drugs.sql";
    private boolean isTr;

    public OpenMRSDrugScript(boolean isTr) {
        this.isTr = isTr;
    }

    public void generate(String inputSrc, String outputDirPath) throws Exception {
        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        System.out.println("Picking Drugs data from:" + inputSrc);
        String outFileName = OPENMRS_DRUGS_SCRIPT;
        System.out.println(String.format("Generating OpenMRS drugs scripts. Output directory: %s/%s", outputDir.getPath(), outFileName));

        File output = new File(outputDir, outFileName);
        URL inputFileUrl = new SHRUtils().getResource(inputSrc);
        CSVParser parser = CSVParser.parse(inputFileUrl, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        List<CSVRecord> csvRecords = parser.getRecords();

        writeLineToFile(output, "\n");

        for (CSVRecord csvRecord : csvRecords) {
            String ignore = csvRecord.get("Ignore");
            if (!StringUtils.isBlank(ignore))
                continue;
            writeLineToFile(output, "START TRANSACTION;");
            writeLineToFile(output, "SET @concept_id = 0;");
            writeLineToFile(output, "SET @dosage_form = 0;");
            writeLineToFile(output, "SET @drug_name_count = 0;");
            writeLineToFile(output, "SET @drug_uuid = 0;");
            writeLineToFile(output, "SET @drug_uri = 0;");

            writeConceptIdCheck(output, csvRecord, "Generic Name", "@concept_id");
            writeConceptIdCheck(output, csvRecord, "Dosage Form", "@dosage_form");
            String brandName = csvRecord.get("Brand Name").trim();
            String strength = csvRecord.get("Strength").trim();
            writeDrugNameCheck(output, brandName, "@drug_name_count");
            writeLineToFile(output,
                    String.format("INSERT INTO drug (concept_id, name, combination, creator, date_created, retired, uuid, strength) " +
                                    " select %s, '%s', 0, 1, now(), 0, uuid(), '%s' from dual where 0 = %s;",
                                    "@concept_id", brandName, strength, "@drug_name_count"));
            writeLineToFile(output, String.format("UPDATE drug set dosage_form = %s where name = '%s' and 0 != @dosage_form; ", "@dosage_form", brandName));
            writeLineToFile(output, String.format("SELECT uuid into @drug_uuid from drug where name = '%s';", brandName));
            if (isTr) {
                writeLineToFile(output, String.format("SELECT concat('%s', @drug_uuid) INTO @drug_uri;", "/openmrs/ws/rest/v1/tr/drugs/"));
                writeLineToFile(output, String.format("INSERT INTO event_records(uuid, title, category, uri, object) values (uuid(), '%s', '%s', @drug_uri, @drug_uri); ", "Medication", "Medication"));
            }
            writeLineToFile(output, "COMMIT;");
            writeLineToFile(output, "\n");
        }

    }

    private void writeDrugNameCheck(File output, String brandName, String varName) throws IOException {
        writeLineToFile(output, String.format("SELECT count(*) into %s from drug where name = '%s';", varName, brandName));
    }

    private void writeConceptIdCheck(File output, CSVRecord csvRecord, String fieldName, final String varName) throws IOException {
        String conceptName = csvRecord.get(fieldName);
        writeLineToFile(output, String.format("SELECT concept_id into %s from " +
                "concept_name where name = '%s' and concept_name_type = 'FULLY_SPECIFIED';", varName, conceptName));
    }


}
