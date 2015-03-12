package org.sharedhealth.hie.data;

public class Main {


    public static String LOCATIONS_DUMP="locations.csv";
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new RuntimeException("Missing argument(s).");
        }


        if ("mci".equals(args[0])) {
            String inputDir = String.format("%s/%s","data",args[1]);
            new MciScript().generate(inputDir, args[2]);
        }
    }
}
