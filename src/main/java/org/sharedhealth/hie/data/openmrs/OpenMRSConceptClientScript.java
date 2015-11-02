package org.sharedhealth.hie.data.openmrs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.sharedhealth.hie.data.SHRUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import static org.sharedhealth.hie.data.Main.OPENMRS_CONCEPT_SCRIPTS;
import static org.sharedhealth.hie.data.SHRUtils.replaceSpecialCharsWithEscapeSequences;
import static org.sharedhealth.hie.data.SHRUtils.writeLineToFile;

public class OpenMRSConceptClientScript {

    private final static String CONCEPT_URL = "/openmrs/ws/rest/v1/tr/concepts/";
    private final static String REFERENCE_TERM_URL = "/openmrs/ws/rest/v1/tr/referenceterms/";
    private final static String CONCEPT_NAME_TYPE_FULLY_SPECIFIED = "FULLY_SPECIFIED";
    private final static String CONCEPT_NAME_TYPE_SHORT = "SHORT";
    private final static String LOCALE_ENGLISH = "en";
    private final static String LOCALE_BANGLA = "bn";
    private boolean isTr;

    public OpenMRSConceptClientScript(boolean isTr) {
        this.isTr = isTr;
    }

    public void generate(String inputDirPath, File outputDir, boolean retainFileName, int prefix) throws Exception {
        generateSQL(inputDirPath, outputDir, retainFileName, prefix);
    }

    private void generateSQL(String inputDirPath, File outputDir, boolean retainFileName, int prefix) throws Exception {
        System.out.println("Picking OpenMRS Concept data from:" + inputDirPath);
        String outFileName = OPENMRS_CONCEPT_SCRIPTS;
        if (retainFileName) {
            outFileName = String.format("%03d_%s.sql", prefix, FilenameUtils.getBaseName(inputDirPath).replace(" ", "_"));
        }
        System.out.println(String.format("Generating OpenMRS concept scripts. Output directory: %s/%s", outputDir.getPath(), outFileName));

        File output = new File(outputDir, outFileName);
        URL inputFileUrl = new SHRUtils().getResource(inputDirPath);
        CSVParser parser = CSVParser.parse(inputFileUrl, Charset.forName("UTF-8"), CSVFormat.newFormat(';').withHeader());
        List<CSVRecord> csvRecords = parser.getRecords();

        writeLineToFile(output, "\n");

        for (CSVRecord csvRecord : csvRecords) {
            writeLineToFile(output, "START TRANSACTION;");
            initializeMysqlVariables(output);
            checkIfConceptExists(output, csvRecord);
            insertIntoConcept(output, csvRecord);
            addConceptToMemberConcept(output, csvRecord);
            addConceptToQuestionConcept(output, csvRecord);
            createReferenceTermMap(output, csvRecord);
            writeLineToFile(output, "COMMIT;");
            writeLineToFile(output, "\n");
        }
    }

    private void initializeMysqlVariables(File output) throws IOException {
        writeLineToFile(output, "SET @concept_id = 0;");
//        writeLineToFile(output, "SET @concept_name_full_id = 0;");
//        writeLineToFile(output, "SET @concept_name_short_id = 0;");
//        writeLineToFile(output, "SET @concept_name_local_full_id = 0;");

        writeLineToFile(output, "SET @data_type_id = 0;");
        writeLineToFile(output, "SET @class_id = 0;");

        writeLineToFile(output, "SET @parent_question_concept_id = 0;");
        writeLineToFile(output, "SET @member_of_concept = 0;");

        writeLineToFile(output, "SET @reference_id = 0;");
        writeLineToFile(output, "SET @concept_source_id = 0;");
        writeLineToFile(output, "SET @concept_map_type_id = 0;");
        writeLineToFile(output, "SET @concept_class_name = 0;");

        writeLineToFile(output, "SET @db_uuid = 0;");
        writeLineToFile(output, "SET @uri = 0;");

        writeLineToFile(output, "SET @should_insert = 0;");
        writeLineToFile(output, "SET @last_concept_id = 0;");
    }

    private void insertIntoConcept(File output, CSVRecord csvRecord) throws IOException {
        String conceptUuid = StringUtils.isNotBlank(csvRecord.get("uuid")) ?
                "'" + StringUtils.trim(csvRecord.get("uuid")) + "'" : "uuid()";
        String conceptName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("name"));
        String conceptShortName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("shortname"));
        String conceptLocalName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("local-name"));
        String conceptDescription = replaceSpecialCharsWithEscapeSequences(csvRecord.get("description"));
        String version = replaceSpecialCharsWithEscapeSequences(getNonMandatoryColumnData(csvRecord, "version"));
        String conceptLocalDescription = replaceSpecialCharsWithEscapeSequences(csvRecord.get("local-description"));
        String conceptClass = StringUtils.trim(csvRecord.get("class"));
        String conceptDatatype = StringUtils.trim(csvRecord.get("datatype"));
        String conceptIsSet = StringUtils.trim(csvRecord.get("is-set"));

        selectConceptDatatype(output, conceptDatatype);
        selectConceptClass(output, conceptClass);
        addConcept(output, conceptUuid, conceptIsSet, version);
        addConceptName(output, conceptName, LOCALE_ENGLISH, "1", CONCEPT_NAME_TYPE_FULLY_SPECIFIED, "@concept_name_full_id");
        addConceptName(output, conceptShortName, LOCALE_ENGLISH, "0", CONCEPT_NAME_TYPE_SHORT, "@concept_name_short_id");
        addConceptName(output, conceptLocalName, LOCALE_BANGLA, "0", CONCEPT_NAME_TYPE_FULLY_SPECIFIED, "@concept_name_local_full_id");
        addConceptSynonym(output, replaceSpecialCharsWithEscapeSequences(getNonMandatoryColumnData(csvRecord, "synonym-1")), LOCALE_ENGLISH, "0");
        addConceptSynonym(output, replaceSpecialCharsWithEscapeSequences(getNonMandatoryColumnData(csvRecord, "synonym-2")), LOCALE_ENGLISH, "0");
        addConceptSynonym(output, replaceSpecialCharsWithEscapeSequences(getNonMandatoryColumnData(csvRecord, "synonym-3")), LOCALE_ENGLISH, "0");
        addConceptDescription(output, conceptDescription, LOCALE_ENGLISH);
        addConceptDescription(output, conceptLocalDescription, LOCALE_BANGLA);
        addConceptNumeric(output, csvRecord);
        addConceptEvent(output, "@concept_id", "'" + conceptClass + "'", false);
    }

    private String getNonMandatoryColumnData(CSVRecord csvRecord, String fieldName) {
        if (!csvRecord.isMapped(fieldName)) return null;
        try {
            return csvRecord.get(fieldName);
        } catch (Exception e) {
            System.out.println(String.format("Error getting data for field %s. Message is %s", fieldName, e.getMessage()));
        }
        return null;
    }

    private void checkIfConceptExists(File output, CSVRecord csvRecord) throws IOException {
        String conceptName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("name"));
        writeLineToFile(output, String.format("SELECT concept_id into @should_insert from " +
                "concept_name where name = '%s' and concept_name_type = '%s';", conceptName, CONCEPT_NAME_TYPE_FULLY_SPECIFIED));
    }

    private void addConceptToQuestionConcept(File output, CSVRecord csvRecord) throws IOException {
        String answerOf = replaceSpecialCharsWithEscapeSequences(csvRecord.get("answer-of"));
        String sortWeight = StringUtils.isNotBlank(csvRecord.get("answer-of-sort-weight")) ?
                StringUtils.trim(csvRecord.get("answer-of-sort-weight")) : "1";

        if (StringUtils.isNotBlank(answerOf)) {
            writeLineToFile(output, String.format("SELECT concept_id INTO @parent_question_concept_id " +
                    "FROM concept_name WHERE name = '%s' " +
                    "AND concept_name_type='%s';", answerOf, CONCEPT_NAME_TYPE_FULLY_SPECIFIED));
            writeLineToFile(output, String.format("INSERT INTO concept_answer (concept_id, answer_concept, answer_drug, date_created, creator, uuid, sort_weight) " +
                            "SELECT @parent_question_concept_id, @concept_id, null, now(), 1, uuid(), %s FROM dual " +
                            "WHERE NOT EXISTS (select ca2.* from concept_answer ca2 where ca2.concept_id = @parent_question_concept_id and ca2.answer_concept = @concept_id) LIMIT 1;",
                    sortWeight));
            addConceptEvent(output, "@parent_question_concept_id", null, true);
        }
    }

    private void addConceptToMemberConcept(File output, CSVRecord csvRecord) throws IOException {
        String memberOf = replaceSpecialCharsWithEscapeSequences(csvRecord.get("member-of"));
        String sortWeight = StringUtils.isNotBlank(csvRecord.get("member-of-sort-weight")) ?
                StringUtils.trim(csvRecord.get("member-of-sort-weight")) : "1";
        if (StringUtils.isNotBlank(memberOf)) {
            writeLineToFile(output, String.format("SELECT concept_id INTO @member_of_concept " +
                    "FROM concept_name WHERE name = '%s' " +
                    "AND concept_name_type='%s';", memberOf, CONCEPT_NAME_TYPE_FULLY_SPECIFIED));
            writeLineToFile(output, String.format("INSERT INTO concept_set (concept_id, concept_set,sort_weight,creator,date_created,uuid) " +
                            "SELECT @concept_id, @member_of_concept, %s,1, now(),uuid() FROM dual " +
                            "WHERE NOT EXISTS (select cs2.* from concept_set cs2 where cs2.concept_id = @concept_id and cs2.concept_set = @member_of_concept) LIMIT 1;",
                    sortWeight));
            addConceptEvent(output, "@member_of_concept", null, true);
        }
    }

    private void selectConceptClassNameFromConceptId(File output, String conceptIdVariable) throws IOException {
        writeLineToFile(output, String.format("SELECT name INTO @concept_class_name from " +
                "concept_class WHERE concept_class_id IN " +
                "(SELECT class_id FROM concept WHERE concept_id = %s);", conceptIdVariable));
    }

    private void addConceptNumeric(File output, CSVRecord csvRecord) throws IOException {
        if (StringUtils.trim(csvRecord.get("datatype")).equalsIgnoreCase("Numeric")) {
            String conceptNumericUnits = replaceSpecialCharsWithEscapeSequences(csvRecord.get("units"));
            String units = StringUtils.isNotBlank(conceptNumericUnits) ? "'" + conceptNumericUnits + "'" : null;
            String lowNormal = StringUtils.isNotBlank(csvRecord.get("low-normal")) ? StringUtils.trim(csvRecord.get("low-normal")) : null;
            String highNormal = StringUtils.isNotBlank(csvRecord.get("high-normal")) ? StringUtils.trim(csvRecord.get("high-normal")) : null;
            String addConceptNumericStatement = String.format("INSERT INTO concept_numeric (concept_id, low_normal, hi_normal, units) " +
                            "SELECT @concept_id, %s, %s, %s FROM dual WHERE 0 = @should_insert;",
                    lowNormal, highNormal,
                    units);
            writeLineToFile(output, addConceptNumericStatement);
        }
    }

    private void addConcept(File output, String conceptUuid, String conceptIsSet, String version) throws IOException {
        String conceptVersion = StringUtils.isBlank(version) ? "0.1" : version.trim();
        String isSet = conceptIsSet.equalsIgnoreCase("true") ? "1" : "0";
        String conceptInsertStmt = String.format("INSERT INTO concept (datatype_id, class_id, is_set, creator, date_created, changed_by, date_changed, uuid, version) " +
                        "SELECT @data_type_id, @class_id, '%s', 1, now(), 1, now(), %s, '%s' FROM dual WHERE 0 = @should_insert;",
                isSet, conceptUuid, conceptVersion);
        writeLineToFile(output, conceptInsertStmt);
        writeLineToFile(output, "SELECT MAX(concept_id) INTO @last_concept_id FROM concept;");
        writeLineToFile(output, "SELECT (case when (@should_insert != 0) THEN @should_insert ELSE @last_concept_id END) INTO @concept_id from dual;");

    }

    private void addConceptDescription(File output, String conceptDescription, String locale) throws IOException {
        if (StringUtils.isNotBlank(conceptDescription)) {
            writeLineToFile(output, String.format("INSERT INTO concept_description(uuid, concept_id, description, locale, creator, date_created) " +
                    "SELECT uuid(), @concept_id, '%s', '%s', 1, now() FROM dual WHERE 0 = @should_insert;", conceptDescription, locale));
        }
    }

    private void selectConceptClass(File output, String conceptClass) throws IOException {
        writeLineToFile(output, String.format("SELECT concept_class_id " +
                        "INTO @class_id FROM concept_class WHERE name = '%s';",
                conceptClass));
    }

    private void selectConceptDatatype(File output, String conceptDatatype) throws IOException {
        writeLineToFile(output, String.format("SELECT concept_datatype_id " +
                        "INTO @data_type_id FROM concept_datatype WHERE name = '%s';",
                conceptDatatype));
    }

    private void addConceptName(File output, String conceptName, final String locale, final String locale_preferred, final String conceptNameType, final String mysqlConceptNameIdVar) throws IOException {
        if (StringUtils.isNotBlank(conceptName)) {
            writeLineToFile(output, String.format("INSERT INTO concept_name (concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, uuid) " +
                            "SELECT @concept_id, '%s', '%s', %s, 1, now(), '%s', uuid() FROM dual WHERE 0 = @should_insert;",
                    conceptName, locale, locale_preferred, conceptNameType));
            // writeLineToFile(output, String.format("SELECT MAX(concept_name_id) INTO %s FROM concept_name;", mysqlConceptNameIdVar));
        }
    }

    private void addConceptSynonym(File output, String conceptName, final String locale, final String locale_preferred) throws IOException {
        if (StringUtils.isNotBlank(conceptName)) {
            writeLineToFile(output, String.format("INSERT INTO concept_name (concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, uuid) " +
                            "SELECT @concept_id, '%s', '%s', %s, 1, now(), null, uuid() FROM dual WHERE 0 = @should_insert;",
                    conceptName, locale, locale_preferred));
        }
    }

    private void addConceptEvent(File output, String conceptIdVariableName, String conceptClassName, boolean raiseEventAnyway) throws IOException {
        if (isTr) {
            if (conceptClassName == null) {
                selectConceptClassNameFromConceptId(output, conceptIdVariableName);
                conceptClassName = "@concept_class_name";
            }
            String shouldInsertVar = "@should_insert";
            if (raiseEventAnyway) {
                shouldInsertVar = "0";
            }
            writeLineToFile(output, String.format("SELECT uuid INTO @db_uuid FROM concept WHERE concept_id = %s;", conceptIdVariableName));
            writeLineToFile(output, String.format("SELECT concat('%s', @db_uuid) INTO @uri;", CONCEPT_URL));
            writeLineToFile(output, String.format("INSERT INTO event_records(uuid, title, category, uri, object) " +
                    "SELECT uuid(), 'concept', 'concept', @uri, @uri FROM dual WHERE 0 = %s;", shouldInsertVar));
            writeLineToFile(output, String.format("INSERT INTO event_records(uuid, title, category, uri, object) " +
                    "SELECT uuid(), %s, %s, @uri, @uri FROM dual WHERE 0 = %s;", conceptClassName, conceptClassName, shouldInsertVar));
        }
    }

    private void createReferenceTermMap(File output, CSVRecord csvRecord) throws IOException {
        String referenceTermSource = StringUtils.trim(csvRecord.get("reference-term-source"));
        String referenceTermCode = replaceSpecialCharsWithEscapeSequences(csvRecord.get("reference-term-code"));
        String referenceTermName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("reference-term-name"));
        String referenceTermRelationship = StringUtils.trim(csvRecord.get("reference-term-relationship"));
        if (StringUtils.isBlank(referenceTermRelationship)) {
            referenceTermRelationship = "SAME-AS";
        }
        String conceptName = replaceSpecialCharsWithEscapeSequences(csvRecord.get("name"));
        if (StringUtils.isBlank(referenceTermName)) {
            referenceTermName = conceptName;
        }
        if (StringUtils.isNotBlank(referenceTermCode)) {
            createReferenceTerm(output, referenceTermSource, referenceTermCode, referenceTermName, referenceTermRelationship);
            writeLineToFile(output, "INSERT INTO concept_reference_map (concept_reference_term_id, concept_map_type_id, creator, date_created, concept_id, uuid) " +
                    "SELECT @reference_id, @concept_map_type_id, 1, now(), @concept_id, uuid() FROM dual WHERE 0 = @should_insert;");
        }
    }

    private void createReferenceTerm(File output, String referenceTermSource, String referenceTermCode, String referenceTermName, String referenceTermRelationship) throws IOException {
        writeLineToFile(output, String.format("SELECT concept_source_id INTO @concept_source_id " +
                "FROM concept_reference_source WHERE name = '%s';", referenceTermSource));
        writeLineToFile(output, String.format("SELECT concept_map_type_id INTO @concept_map_type_id " +
                "FROM concept_map_type WHERE name = '%s';", referenceTermRelationship));
        writeLineToFile(output, String.format("SELECT concept_reference_term_id INTO @reference_id FROM concept_reference_term " +
                "WHERE code = '%s' AND concept_source_id = @concept_source_id;", referenceTermCode));
        writeLineToFile(output, String.format("INSERT INTO concept_reference_term (concept_source_id, name, code, creator, date_created, uuid) " +
                        "SELECT @concept_source_id, '%s', '%s', 1, now(), uuid() FROM dual WHERE 0 = @should_insert AND 0 = @reference_id;",
                referenceTermName, referenceTermCode));
        writeLineToFile(output, String.format("SELECT concept_reference_term_id INTO @reference_id FROM concept_reference_term " +
                "WHERE code = '%s' AND concept_source_id = @concept_source_id;", referenceTermCode));
        addReferenceTermEvent(output);
    }

    private void addReferenceTermEvent(File output) throws IOException {
        if (isTr) {
            writeLineToFile(output, "SELECT uuid INTO @db_uuid FROM concept_reference_term WHERE concept_reference_term_id = @reference_id;");
            writeLineToFile(output, String.format("SELECT concat('%s', @db_uuid) INTO @uri;", REFERENCE_TERM_URL));
            writeLineToFile(output, "INSERT INTO event_records(uuid, title, category, uri, object) " +
                    "SELECT uuid(), 'ConceptReferenceTerm', 'ConceptReferenceTerm', @uri, @uri FROM dual WHERE 0 = @should_insert;");
        }
    }
}