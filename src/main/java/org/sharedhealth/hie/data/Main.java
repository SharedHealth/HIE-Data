package org.sharedhealth.hie.data;

import org.sharedhealth.hie.data.bahmni.BDSHRClientScript;
import org.sharedhealth.hie.data.mci.MciScript;

public class Main {


    public static String LOCATIONS_DATA = "locations.csv";
    public static String FACILITIES_DATA = "facilities.csv";
    public static String PROVIDERS_DATA = "providers.csv";

    public static String LOCATIONS_SCRIPTS = "locations.cql";
    public static String FACILITIES_SCRIPTS = "facilities.cql";
    public static String PROVIDERS_SCRIPTS = "providers.cql";

    public static String HRM_TEST="http://hrmtest.dghs.gov.bd";
    public static String HRM_PROD="http://hrm.dghs.gov.bd";
    public static String HRM = HRM_TEST;
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Please use the below format");
            System.out.println("java -jar <<project-name|mci|shr-client>> <<env|prod,qa,showcase>> <<output-dir-path>>");
            throw new RuntimeException("Missing argument(s).");
        }

        String proj = args[0];
        String env = args[1];
        String outputDir = String.format("%s/%s", args[2], proj);
        String inputDir = String.format("%s/%s", "data", env);

        HRM = "prod".equals(env)? HRM_PROD:HRM_TEST;

        if ("mci".equals(proj)) {
            new MciScript().generate(inputDir, outputDir);
        }

        if ("shr-client".equals(proj)){
            new BDSHRClientScript().generate(inputDir, outputDir);
        }
    }
}
