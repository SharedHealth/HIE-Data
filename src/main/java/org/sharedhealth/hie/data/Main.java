package org.sharedhealth.hie.data;

public class Main {


    public static String LOCATIONS_DATA ="locations.csv";
    public static String LOCATIONS_SCRIPTS ="locations.cql";
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Please use the below format");
            System.out.println("java -jar <<project-name|mci>> <<env|prod,qa,showcase>> <<output-dir-path>>");
            throw new RuntimeException("Missing argument(s).");
        }


        if ("mci".equals(args[0])) {
            String inputDir = String.format("%s/%s","data",args[1]);
            new MciScript().generate(inputDir, args[2]);
        }
    }
}
