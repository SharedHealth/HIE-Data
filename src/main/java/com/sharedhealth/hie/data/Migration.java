package com.sharedhealth.hie.data;

import java.io.IOException;

public class Migration {


    public static String LOCATIONS_DUMP = "locations.csv";

    public static void main(String[] args) throws IOException {
        if(args[0] == "mci"){
            new MCIData().run();
        }
    }


}
