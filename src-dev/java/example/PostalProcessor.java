package example;

import javax.batch.api.chunk.ItemProcessor;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kawasima
 */
public class PostalProcessor implements ItemProcessor {
    @Override
    public Object processItem(Object item) throws Exception {
        Map<String,String> original = (Map<String, String>) item;
        Map<String, String> converted = new HashMap<>();
        converted.put("postalCd", original.get("postalCd"));
        converted.put("prefectureName", original.get("prefectureName"));
        converted.put("prefectureKana",
                Normalizer.normalize(original.get("prefectureKana"), Normalizer.Form.NFKC));
        return converted;
    }
}
