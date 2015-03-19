package org.sharedhealth.hie.data;

import java.net.URL;

public class SHRFileUtils {

    public URL getResource(String resource) throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return classLoader.getResource(resource).toURI().toURL();
    }
}
