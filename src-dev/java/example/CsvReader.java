package example;

import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;

import javax.batch.api.chunk.ItemReader;
import java.io.*;
import java.nio.charset.Charset;

/**
 * @author kawasima
 */
public class CsvReader implements ItemReader {
    CsvMapReader reader;
    String[] header = {
            "localCd", "oldPostalCd", "postalCd",
            "prefectureKana", "cityKana", "townKana",
            "prefectureName", "cityName", "townName",
            "a", "b", "c", "d", "e", "f"
    };
    @Override
    public void open(Serializable checkpoint) throws Exception {
        InputStream in = new FileInputStream(new File("/home/kawasima/workspace/job-streamer/job-streamer-control-bus/resources/KEN_ALL.csv"));
        reader = new CsvMapReader(
                new InputStreamReader(in, Charset.forName("Windows-31J")),
                CsvPreference.EXCEL_PREFERENCE);
    }

    @Override
    public void close() throws Exception {
        if (reader != null)
            reader.close();
    }

    @Override
    public Object readItem() throws Exception {
        return reader.read(header);
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
