package example;

import org.supercsv.io.CsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import javax.batch.api.chunk.ItemWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author kawasima
 */
public class CsvWriter implements ItemWriter {
    CsvMapWriter writer;
    String[] header = {"postalCd", "prefectureName", "prefectureKana"};

    @Override
    public void open(Serializable checkpoint) throws Exception {
        writer = new CsvMapWriter(new FileWriter("target/prefectures.csv"),
                CsvPreference.TAB_PREFERENCE);
        writer.writeHeader(header);
    }

    @Override
    public void close() throws Exception {
        if (writer != null)
            writer.close();
    }

    @Override
    public void writeItems(List<Object> items) throws Exception {
        for(Object item : items) {
            writer.write((Map<String, String>) item, header);
        }
    }

    @Override
    public Serializable checkpointInfo() throws Exception {
        return null;
    }
}
