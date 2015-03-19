package org.sharedhealth.hie.data;


import org.junit.Test;

public class HIEDataTest {

    @Test
    public void shouldGenerateMCIScriptsFromCSV() throws Exception {
        new MciScript().generate("data/test", "./scripts");
    }

    @Test
    public void shouldGenerateSHRClientScriptsFromCSV() throws Exception {
        new BDSHRClientScript().generate("data/test", "./scripts");

    }
}