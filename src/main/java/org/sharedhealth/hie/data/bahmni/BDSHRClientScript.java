package org.sharedhealth.hie.data.bahmni;

public class BDSHRClientScript {

    public void generate(String inputDir, String outputDirPath) throws Exception {
        new LRDataSet().generate(inputDir, outputDirPath);
    }



}
