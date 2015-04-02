package org.sharedhealth.hie.data;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.sharedhealth.hie.data.Main.HRM;

public class SHRUtils {

    public URL getResource(String resource) throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return classLoader.getResource(resource).toURI().toURL();
    }

    public void updateMarkersForSHRClient(int lastReadEntryId, String feedUri, String uriFragment, File output) throws IOException {

        String feedUriForLastReadEntry = getFeedUriForLastReadEntry(uriFragment);

        String updateMarker = String.format("update markers set last_read_entry_id='%s', feed_uri_for_last_read_entry = '%s' where feed_uri='%s';\n",
                lastReadEntryId, feedUriForLastReadEntry, feedUri);
        FileUtils.writeStringToFile(output, updateMarker, Charset.forName("UTF-8"), true);
    }

    public void updateMarkersForMCI(String type, String uriFragment, File output) throws IOException {
        String feedUriForLastReadEntry = getFeedUriForLastReadEntry(uriFragment);

        String updateMarker = String.format("update lr_markers set last_feed_url='%s' where type='%s';\n",
                feedUriForLastReadEntry, type);
        FileUtils.writeStringToFile(output, updateMarker, Charset.forName("UTF-8"), true);

    }

    private String getFeedUriForLastReadEntry(String uriFragment) {
        return String.format("%s/api/1.0/%s?offset=0&limit=100&updatedSince=%s", HRM, uriFragment, getCurrentDateAndTime());
    }

    private String getCurrentDateAndTime() {
        return new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date());
    }
}
