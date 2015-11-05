package org.sharedhealth.hie.data.openmrs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;

public class OMRSClientScript {
    private boolean shouldRaiseEvent;
    private boolean isBahmni;

    public OMRSClientScript(boolean shouldRaiseEvent, boolean isBahmni) {
        this.shouldRaiseEvent = shouldRaiseEvent;
        this.isBahmni = isBahmni;
    }

    public void generate(String inputDirPath, String outputDirPath) throws Exception {
        File outputDir = new File(outputDirPath);
        outputDir.mkdirs();
        FileUtils.cleanDirectory(outputDir);

        if (!StringUtils.isBlank(inputDirPath) && inputDirPath.equals("TR_ALL")) {
            System.out.println("Trying to generate all TR concepts");
            generateForAllConfigured(outputDir, "data/openmrs-concept/tr/tr_all_concepts.txt", "data/openmrs-concept/tr/tr_all_drugs.txt");
        } else if (!StringUtils.isBlank(inputDirPath) && inputDirPath.equals("BAHMNI_ALL")) {
            System.out.println("Trying to generate all Bahmni concepts");
            generateForAllConfigured(outputDir, "data/openmrs-concept/bahmni/bahmni_all_files.txt", null);
        } else {
            new OpenMRSConceptClientScript(shouldRaiseEvent, isBahmni).generate(inputDirPath, outputDir, false, 1);
        }
    }

    private void generateForAllConfigured(File outputDir, String config, String drug_config_path) throws Exception {
        OpenMRSConceptClientScript conceptClientScript = new OpenMRSConceptClientScript(shouldRaiseEvent, isBahmni);
        int i = 1;
        List<String> lines = IOUtils.readLines(ClassLoader.getSystemResourceAsStream(config));
        for (String line : lines) {
            if (StringUtils.isBlank(line)) {
                continue;
            }
            if (!line.startsWith("#")) {
                System.out.println("Processing entry: " + line);
                conceptClientScript.generate(line, outputDir, true, i);
                i++;
            }
        }
        if (null == drug_config_path) return;
        List<String> drugFileNames = IOUtils.readLines(ClassLoader.getSystemResourceAsStream(drug_config_path));
        OpenMRSDrugScript openMRSDrugScript = new OpenMRSDrugScript(shouldRaiseEvent);
        for (String drugFileName : drugFileNames) {
            if (StringUtils.isBlank(drugFileName)) {
                continue;
            }
            if (!drugFileName.startsWith("#")) {
                System.out.println("Processing entry: " + drugFileName);
                openMRSDrugScript.generate(drugFileName, outputDir, true, i);
                i++;
            }
        }
    }
}
