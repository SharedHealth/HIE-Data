package org.sharedhealth.hie.data;

import org.sharedhealth.hie.data.bahmni.BDSHRClientScript;
import org.sharedhealth.hie.data.mci.MciScript;
import org.sharedhealth.hie.data.openmrs.OMRSClientScript;
import org.sharedhealth.hie.data.openmrs.OpenMRSDrugScript;
import org.sharedhealth.hie.data.openmrs.OpenMRSTestMapScript;

public class Main {


    public static String LOCATIONS_DATA = "locations.csv";
    public static String FACILITIES_DATA = "facilities.csv";
    public static String PROVIDERS_DATA = "providers.csv";

    public static String LOCATIONS_SCRIPTS_CQL = "locations.cql";

    public static String LOCATIONS_SCRIPTS = "locations.sql";
    public static String FACILITIES_SCRIPTS = "facilities.sql";
    public static String PROVIDERS_SCRIPTS = "providers.sql";

    public static String OPENMRS_CONCEPT_SCRIPTS = "openrms_concept_data.sql";

    public static String HRM_TEST = "http://hrmtest.dghs.gov.bd";
    public static String HRM_PROD = "http://hrm.dghs.gov.bd";
    public static String HRM = HRM_TEST;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Please specify the project name");
            System.out.println("java -jar <<project-name|mci|shr-client|openmrs-concept>>");
            throw new RuntimeException("Missing argument(s).");
        }

        String proj = args[0];

        if ("mci".equals(proj)) {
            generateMciScripts(args, proj);
        }

        if ("shr-client".equals(proj)) {
            generateShrClientHrmScripts(args, proj);
        }

        if ("openmrs-concept".equals(proj)) {
            generateOpenmrsConceptScripts(args);
        }

        if ("drugs".equals(proj)) {
            generateDrugScripts(args);
        }

        if ("openmrs-test-map".equals(proj)) {
            generateOpenmrsConceptScriptsForElis(args);
        }
    }

    private static void generateOpenmrsConceptScriptsForElis(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Please use the below format");
            System.out.println("java -jar openmrs-concept <<input-dir-path>> <<output-dir-path>> <<is TR server: true|false>>");
            throw new RuntimeException("Missing argument(s).");
        }
        String inputDir = args[1];
        String outputDir = args[2];
        boolean shouldRaiseEvents = new Boolean(args[3]);
        boolean isBahmni = false;
        if (args.length > 4) {
            isBahmni = new Boolean(args[4]);
        }
        new OpenMRSTestMapScript(true).generate(inputDir, outputDir);
    }

    private static void generateDrugScripts(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Please use the below format");
            System.out.println("java -jar drugs <<input-dir-path>> <<output-dir-path>> <<is TR server: true|false>>");
            throw new RuntimeException("Missing argument(s).");
        }
        String inputDir = args[1];
        String outputDir = args[2];
        boolean shouldRaiseEvents = new Boolean(args[3]);
        new OpenMRSDrugScript(shouldRaiseEvents).generate(inputDir, outputDir);
    }

    private static void generateOpenmrsConceptScripts(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Please use the below format");
            System.out.println("java -jar openmrs-concept <<input-dir-path>> <<output-dir-path>> <<is TR server: true|false>>");
            throw new RuntimeException("Missing argument(s).");
        }
        String inputDir = args[1];
        String outputDir = args[2];
        boolean shouldRaiseEvents = new Boolean(args[3]);
        boolean isBahmni = false;
        if (args.length > 4) {
            isBahmni = new Boolean(args[4]);
        }
        new OMRSClientScript(shouldRaiseEvents,isBahmni).generate(inputDir, outputDir);
    }

    private static void generateShrClientHrmScripts(String[] args, String proj) throws Exception {
        if (args.length < 3) {
            System.out.println("Please use the below format");
            System.out.println("java -jar <<mci>> <<env|prod,qa,showcase>> <<output-dir-path>>");
            throw new RuntimeException("Missing argument(s).");
        }
        String env = args[1];
        String outputDir = String.format("%s/%s", args[2], proj);
        String inputDir = String.format("%s/%s", "data", env);

        HRM = "prod".equals(env) ? HRM_PROD : HRM_TEST;

        new BDSHRClientScript().generate(inputDir, outputDir);
    }

    private static void generateMciScripts(String[] args, String proj) throws Exception {
        if (args.length < 3) {
            System.out.println("Please use the below format");
            System.out.println("java -jar <<shr-client>> <<env|prod,qa,showcase>> <<output-dir-path>>");
            throw new RuntimeException("Missing argument(s).");
        }
        String env = args[1];
        String outputDir = String.format("%s/%s", args[2], proj);
        String inputDir = String.format("%s/%s", "data", env);

        HRM = "prod".equals(env) ? HRM_PROD : HRM_TEST;

        new MciScript().generate(inputDir, outputDir);
    }
}
