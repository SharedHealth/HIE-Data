package org.sharedhealth.hie.data.bahmni;

import org.apache.commons.io.FileUtils;

import java.io.File;

public class BDSHRClientScript {

    public void generate(String inputDir, String outputDirPath) throws Exception {
        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        new LRDataSet().generate(inputDir, outputDir);
        new FRDataSet().generate(inputDir, outputDir);
        new PRDataSet().generate(inputDir, outputDir);
    }



}
