package org.sharedhealth.hie.data;


import org.junit.Test;
import org.sharedhealth.hie.data.bahmni.FRDataSet;
import org.sharedhealth.hie.data.bahmni.LRDataSet;
import org.sharedhealth.hie.data.bahmni.PRDataSet;
import org.sharedhealth.hie.data.mci.MciScript;

public class HIEDataTest {

    @Test
    public void shouldGenerateMCIScriptsFromCSV() throws Exception {
        new MciScript().generate("data/test", "./scripts");
    }

    @Test
    public void shouldGenerateBahmniLocationDataFromCSV() throws Exception {
        new LRDataSet().generate("data/test", "./scripts");

    }

    @Test
    public void shouldGenerateFacilityDataFromCSV() throws Exception {
        new FRDataSet().generate("data/test","./scripts");
    }

    @Test
    public void shouldGenerateProviderDataFromCSV() throws Exception {
        new PRDataSet().generate("data/test","./scripts");
    }
}