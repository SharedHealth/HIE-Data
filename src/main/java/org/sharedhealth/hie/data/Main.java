package org.sharedhealth.hie.data;

import java.io.IOException;

public class Main {

    public static String LOCATIONS_DUMP = "data/locations.csv";

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new RuntimeException("Missing argument(s).");
        }

        if ("mci".equals(args[0])) {
            MciSetup setup = new MciSetup();
            String action = args[1];

            if ("all".equals(action)) {
                setup.generateScripts();
                setup.applyScripts();

            } else if ("generate".equals(action)) {
                setup.generateScripts();

            } else if ("apply".equals(action)) {
                setup.applyScripts();
            }
        }
    }
}
