package org.sharedhealth.hie.data;

import java.io.IOException;

public class Main {

    public static String LR_DUMP = "data/locations.csv";

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new RuntimeException("Missing argument(s).");
        }

        if ("mci".equals(args[0])) {
            new MciScript().generate(args[1]);
        }
    }
}
