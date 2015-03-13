package org.sharedhealth.hie.data;


import org.junit.Test;

public class MciScriptTest {

    @Test
    public void shouldGenerateSqlScriptsFromCSV() throws Exception {
        new MciScript().generate("data/test", "./scripts");
    }

}