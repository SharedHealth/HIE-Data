package org.sharedhealth.hie.data;


import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.sharedhealth.hie.data.bahmni.FRDataSet;
import org.sharedhealth.hie.data.bahmni.LRDataSet;
import org.sharedhealth.hie.data.bahmni.PRDataSet;
import org.sharedhealth.hie.data.mci.MciScript;

import java.io.*;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class HIEDataTest {

    @Test
    public void shouldGenerateMCIScriptsFromCSV() throws Exception {
        new MciScript().generate("data/test", "./scripts");
    }

    @Test
    public void shouldGenerateBahmniLocationDataFromCSV() throws Exception {
        new LRDataSet().generate("data/test", new File("./scripts"));

    }

    @Test
    public void shouldGenerateFacilityDataFromCSV() throws Exception {
        new FRDataSet().generate("data/test", new File("./scripts"));
    }

    @Test
    public void shouldGenerateProviderDataFromCSV() throws Exception {
        new PRDataSet().generate("data/test", new File("./scripts"));
    }

    @Test
    @Ignore
    public void generateTRAdminPriv() throws Exception {
        SHRUtils shrUtils = new SHRUtils();
        URL resource = shrUtils.getResource("data/tr_admin_privileges.text");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.openStream()))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println(String.format("INSERT INTO role_privilege (role, privilege) values ('TR_ADMIN', '%s');", line.trim()));
            }
        }

    }

    @Test
    @Ignore
    public void cleanupDatasheet() throws Exception {
        InputStream inputStream = new URL("file:///Users/angshus/Downloads/formulation_pilot_tr_template.csv").openStream();
        Writer writer = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        String line = null;
        try {
            inputStreamReader = new InputStreamReader(inputStream, "iso-8859-1");
            bufferedReader = new BufferedReader(inputStreamReader);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/angshus/Documents/formulation_pilot_tr_template.csv"), "utf-8"));
            while ((line = bufferedReader.readLine()) != null) {
                writer.write(line.replace("/?", "/").concat("\n"));
            }
        } finally {
            if (bufferedReader != null)
                bufferedReader.close();

            if (inputStreamReader != null)
                inputStreamReader.close();

            if (inputStream != null)
                inputStream.close();

            if (writer != null)
                writer.close();
        }

    }

    @Test
    @Ignore
    public void shouldConvertSamplesLabTestCSVIntoAppropriateFormat() throws Exception {
        InputStream inputStream = new URL("file:////Users/mritunjd/Downloads/Masterlist of tests - Samples.csv").openStream();
        Writer writer = null;
        ArrayList<String> conceptLines = new ArrayList<>();
        conceptLines.add("uuid;name;description;local-name;local-description;class;datatype;shortname;is-set;reference-term-source;reference-term-code;reference-term-name;reference-term-relationship;units;high-normal;low-normal;member-of;member-of-sort-weight;answer-of;answer-of-sort-weight");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                if (i++ == 0) continue;
                conceptLines.addAll(generateConceptSetLines(line, "Sample", "Lab Samples"));
            }
            String csvContents = StringUtils.join(conceptLines, "\n");
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/mritunjd/Documents/lab_test_samples.csv"), "utf-8"));
            writer.write(csvContents);
        }finally {
            if (inputStream != null)
                inputStream.close();

            if (writer != null)
                writer.close();
        }
    }

    @Test
    @Ignore
    public void shouldConvertDepartmentLabTestCSVIntoAppropriateFormat() throws Exception {
        InputStream inputStream = new URL("file:////Users/mritunjd/Downloads/Masterlist of tests - Depts.csv").openStream();
        Writer writer = null;
        ArrayList<String> conceptLines = new ArrayList<>();
        conceptLines.add("uuid;name;description;local-name;local-description;class;datatype;shortname;is-set;reference-term-source;reference-term-code;reference-term-name;reference-term-relationship;units;high-normal;low-normal;member-of;member-of-sort-weight;answer-of;answer-of-sort-weight");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                if (i++ == 0) continue;
                conceptLines.addAll(generateConceptSetLines(line, "Department", "Lab Departments"));
            }
            String csvContents = StringUtils.join(conceptLines, "\n");
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/mritunjd/Documents/lab_test_department.csv"), "utf-8"));
            writer.write(csvContents);
        }finally {
            if (inputStream != null)
                inputStream.close();

            if (writer != null)
                writer.close();
        }
    }

    private ArrayList<String> generateConceptSetLines(String line, final String className, final String memberOf) {
        String[] words = line.split(",");
        ArrayList<String> conceptLines = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            if (i == 0) {
                String conceptName = words[0].trim();
                String conceptLine = String.format(";%s;;;;%s;N/A;%s;TRUE;;;;;;;;%s;;;", conceptName + " " + className, className, conceptName, memberOf);
                conceptLines.add(conceptLine);
            } else {
                String conceptSetName = words[0].trim();
                String conceptName = words[i].trim();
                String conceptLine = String.format(";%s;;;;LabTest;N/A;%s;FALSE;;;;;;;;%s;;;", conceptName, conceptName, conceptSetName+ " " + className);
                conceptLines.add(conceptLine);
            }
        }
        return conceptLines;
    }

    @Test
    @Ignore
    public void lookupLoincFSN() throws Exception {
        InputStream inputStream = new URL("file:///Users/angshus/Downloads/Tests in our template.csv").openStream();
        Writer writer = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        String line = null;
        String lineToWrite = null;
        Connection conn = null;
        PreparedStatement statement = null;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/loinc?user=root&password=password");
            statement = conn.prepareStatement(
                    "select concat(component, ': ', property, ': ', time_aspct, ': ', system, ': ', scale_typ, ': ', method_typ) as fsn  " +
                            "from loinc where loinc_num = ?");

            inputStreamReader = new InputStreamReader(inputStream, "iso-8859-1");
            bufferedReader = new BufferedReader(inputStreamReader);
            int i = 0;
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/angshus/Documents/test_with_fsn.csv"), "utf-8"));
            while ((line = bufferedReader.readLine()) != null) {
                if (i > 0) {
                    String[] parts = line.split(";");
                    String loincNumberSpecified = parts[10];
                    String conceptClass = parts[5].trim();
                    if ((!StringUtils.isBlank(loincNumberSpecified)) && (StringUtils.equals(conceptClass, "Test"))) {
                        String loincCode = loincNumberSpecified.trim().replaceAll("[^-0-9]", "");
                        lineToWrite = line.replace(loincNumberSpecified, loincCode).replace("/?", "/");
                        statement.setString(1, loincCode);
                        ResultSet resultSet = statement.executeQuery();
                        if (resultSet.next()) {
                            String fsn = resultSet.getString("fsn");
                            writer.write(lineToWrite.concat(";" + fsn + "\n"));
                        } else {
                            System.out.println(String.format("Could not fetch FSN for code [%s]. Row [%s]", loincCode, i + 1));
                            writer.write(lineToWrite.concat(";" + "\n"));
                        }
                        resultSet.close();
                    } else {
                        //System.out.println(String.format("Could not fetch FSN for row [%s]. Possibly unspecified code [%s] or not appropriate class [%s]", i+1, loincNumberSpecified, conceptClass));
                        writer.write(line.replace("/?", "/").concat(";" + "\n"));
                    }
                } else {
                    writer.write(line + ";FSN" + "\n");
                }
                i++;
            }
        } finally {
            if (bufferedReader != null)
                bufferedReader.close();

            if (inputStreamReader != null)
                inputStreamReader.close();

            if (inputStream != null)
                inputStream.close();

            if (writer != null)
                writer.close();

            if (statement != null)
                statement.close();

            if (conn != null)
                conn.close();
        }

    }

}