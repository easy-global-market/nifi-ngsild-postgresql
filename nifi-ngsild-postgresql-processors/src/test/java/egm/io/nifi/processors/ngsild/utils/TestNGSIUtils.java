package egm.io.nifi.processors.ngsild.utils;

import org.json.JSONArray;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TestNGSIUtils {

    private String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    private final InputStream inputStream = getClass().getClassLoader().getResourceAsStream("temporalEntities.json");

    private final NGSIUtils ngsiUtils = new NGSIUtils();

    @Test
    public void testTemporalEntities() throws IOException {
        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));
        assertEquals(2, entities.size());

        ArrayList<Attributes> attributes = entities.get(0).entityAttrsLD;
        Map<String, List<Attributes>> attributesByObservedAt = attributes.stream().collect(Collectors.groupingBy(attrs -> attrs.observedAt));
        assertEquals(3, attributesByObservedAt.size());
    }

    @Test
    public void verifyIfAttributesAreCompliant() throws IOException {
        String data = readFromInputStream(inputStream);
        ArrayList<Entity> entities = ngsiUtils.parseNgsiLdEntities(new JSONArray(data));
        assertTrue(
                entities.stream().allMatch(
                        entity -> entity.entityAttrsLD.stream()
                                .allMatch(attributes ->
                                        !Objects.equals(attributes.attrName, "nullValue") &&
                                            !Objects.equals(attributes.attrName, "nullAttribute")
                                )
                )
        );
    }

}
