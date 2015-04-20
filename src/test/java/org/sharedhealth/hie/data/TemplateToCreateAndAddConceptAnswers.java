package org.sharedhealth.hie.data;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

// NOTE : this file generates liquibase changeset to create and add concept answers as per our requirement
// The output.xml file holds the changeset to be copied from and pasted in liquibese files
// in the input.txt file every line should consist the following information : concept_full_name, conpept_short_name,
// search_string_one and search_string_two
// TODO : when you run this, make sure to delete the output.xml file
public class TemplateToCreateAndAddConceptAnswers {
    public static final String CHANGESET_PREFIX = "BD";
    public static final String CHANGESET_ID_FORMAT = "yyyyMMdd-HHmm";
    public static final String AUTHOR = "Utsab, Hitansu";
    public static final String INPUT_FILE_NAME = "input.txt";
    public static final String OUTPUT_FILE_NAME = "output.xml";
    public static final String PARENT_CONCEPT_NAME = "Occupation";
    public static final int FULLNAME_INDEX = 0;
    public static final int SHORTNAME_INDEX = 1;
    public static final int SEARCH_STRING_ONE_INDEX = 2;
    public static final int SEARCH_STRING_TWO_INDEX = 3;

    @Test
    public void generateChangeSet() throws IOException {
        String template = "    <changeSet id=\"%s-%s-%s\" author=\"%s\">\n" +
                "        <preConditions onFail=\"MARK_RAN\">\n" +
                "            <sqlCheck expectedResult=\"0\">\n" +
                "                select count(*) from concept_name where name = '%s';\n" +
                "            </sqlCheck>\n" +
                "        </preConditions>\n" +
                "        <comment>Adding Concept %s</comment>\n" +
                "        <sql>\n" +
                "            set @concept_id = 0;\n" +
                "            set @answer_concept_id = 0;\n" +
                "            set @concept_name_short_id = 0;\n" +
                "            set @concept_name_full_id = 0;\n" +
                "            SELECT concept_id INTO @concept_id FROM concept_name WHERE name='%s'  and concept_name_type = 'short';\n" +
                "            call add_concept(@answer_concept_id, @concept_name_short_id, @concept_name_full_id, '%s', '%s', 'Text', 'Misc', false);\n" +
                "            call add_concept_word(@answer_concept_id, @concept_name_short_id, '%s', '1');\n" +
                "            call add_concept_word(@answer_concept_id, @concept_name_full_id, '%s', '1');\n" +
                "            call add_concept_answer(@concept_id, @answer_concept_id, %s);\n" +
                "        </sql>\n" +
                "    </changeSet>\n";
        BufferedReader bufferedReader = getFileInputHandler(INPUT_FILE_NAME);
        BufferedWriter bufferedWriter = getFileOutputHandler(OUTPUT_FILE_NAME);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(CHANGESET_ID_FORMAT);
        String line;
        int id = 0;
        int sortId = 1;

        while ((line = bufferedReader.readLine()) != null) {
            String[] inputs = line.split("//");
            if (inputs.length != 4) {
                throw new RuntimeException(String.format("Invalid input at input file [line no : %s]", sortId));
            }
            String fullName = inputs[FULLNAME_INDEX].trim();
            String shortName = inputs[SHORTNAME_INDEX].trim();
            String searchStringOne = inputs[SEARCH_STRING_ONE_INDEX].trim();
            String searchStringTwo = inputs[SEARCH_STRING_TWO_INDEX].trim();
            String changeSet = String.format(template, CHANGESET_PREFIX, simpleDateFormat.format(new Date()), id++, AUTHOR,
                    fullName, fullName, PARENT_CONCEPT_NAME, fullName, shortName, searchStringOne, searchStringTwo, sortId++);
            bufferedWriter.write(changeSet);
        }
        bufferedReader.close();
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    private BufferedWriter getFileOutputHandler(String outputFileName) throws IOException {
        File file = new File(outputFileName);
        FileWriter fileWriter = new FileWriter(file);
        return new BufferedWriter(fileWriter);
    }

    private BufferedReader getFileInputHandler(String inputFileName) {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(inputFileName);
        return new BufferedReader(new InputStreamReader(inputStream));
    }
}
