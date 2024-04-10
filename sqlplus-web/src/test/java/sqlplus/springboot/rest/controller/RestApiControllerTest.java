package sqlplus.springboot.rest.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import sqlplus.springboot.Application;
import sqlplus.springboot.dto.Result;
import sqlplus.springboot.rest.request.ParseQueryRequest;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = Application.class)
@AutoConfigureMockMvc
public class RestApiControllerTest {
    @Autowired
    private MockMvc mvc;

    @Test
    public void testParseAggregationQuery() throws Exception {
        String ddl = "CREATE TABLE Graph\n" +
                "(\n" +
                "    src INT,\n" +
                "    dst INT\n" +
                ")";
        String query = "SELECT COUNT(*)\n" +
                "FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4\n" +
                "WHERE g1.dst = g2.src AND g2.dst = g3.src AND g3.dst = g4.src AND g1.src < g4.dst\n" +
                "GROUP BY g2.src, g3.dst";

        ParseQueryRequest request = new ParseQueryRequest();
        request.setDdl(ddl);
        request.setQuery(query);

        ObjectMapper objectMapper = new ObjectMapper();
        String body = objectMapper.writeValueAsString(request);

        String response = mvc.perform(MockMvcRequestBuilders
                .post("http://localhost:8848/api/v1/parse")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
                .andReturn().getResponse().getContentAsString();
        Result result = objectMapper.readValue(response, Result.class);

        assertEquals(200, result.getCode());
        Map<String, Object> data = (Map<String, Object>) result.getData();

        // tables
        List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
        assertEquals(1, tables.size());
        assertEquals("Graph", tables.get(0).get("name"));
        assertEquals(2, ((List<String>) tables.get(0).get("columns")).size());
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("src"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("dst"));

        // joinTrees
        assertEquals(2, ((List<Map<String, Object>>) data.get("joinTrees")).size());

        // computations
        assertEquals(0, ((List<Object>) data.get("computations")).size());

        // outputVariables
        assertEquals(1, ((List<String>) data.get("outputVariables")).size());

        // groupByVariables
        assertEquals(2, ((List<String>) data.get("groupByVariables")).size());

        // aggregations
        assertEquals(1, ((List<Map<String, String>>) data.get("aggregations")).size());
        Map<String, Object> aggregation = ((List<Map<String, Object>>) data.get("aggregations")).get(0);
        assertEquals("COUNT", aggregation.get("func"));
        assertEquals(0, ((List<Object>) (aggregation.get("args"))).size());

        // topK
        assertNull(data.get("topK"));

        // isFull
        assertFalse((Boolean) data.get("full"));

        // isFreeConnex
        assertFalse((Boolean) data.get("freeConnex"));
    }

    @Test
    public void testParseTopKQuery() throws Exception {
        String ddl = "CREATE TABLE Graph\n" +
                "(\n" +
                "    src    INT,\n" +
                "    dst    INT,\n" +
                "    rating DECIMAL\n" +
                ")";
        String query = "SELECT R.src AS node1, S.src AS node2, S.dst AS node3, R.rating + S.rating AS total_rating\n" +
                "FROM graph R,\n" +
                "     graph S\n" +
                "WHERE R.dst = S.src\n" +
                "ORDER BY total_rating DESC limit 5";

        ParseQueryRequest request = new ParseQueryRequest();
        request.setDdl(ddl);
        request.setQuery(query);

        ObjectMapper objectMapper = new ObjectMapper();
        String body = objectMapper.writeValueAsString(request);

        String response = mvc.perform(MockMvcRequestBuilders
                        .post("http://localhost:8848/api/v1/parse")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andReturn().getResponse().getContentAsString();
        Result result = objectMapper.readValue(response, Result.class);

        assertEquals(200, result.getCode());
        Map<String, Object> data = (Map<String, Object>) result.getData();

        // tables
        List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
        assertEquals(1, tables.size());
        assertEquals("Graph", tables.get(0).get("name"));
        assertEquals(3, ((List<String>) tables.get(0).get("columns")).size());
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("src"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("dst"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("rating"));

        // joinTrees
        assertEquals(2, ((List<Map<String, Object>>) data.get("joinTrees")).size());

        // computations
        assertEquals(1, ((List<Map<String, String>>) data.get("computations")).size());

        // outputVariables
        assertEquals(4, ((List<String>) data.get("outputVariables")).size());

        // groupByVariables
        assertEquals(0, ((List<Object>) data.get("groupByVariables")).size());

        // aggregations
        assertEquals(0, ((List<Object>) data.get("aggregations")).size());

        // topK
        Map<String, Object> topK = (Map<String, Object>) data.get("topK");
        assertTrue((Boolean) topK.get("desc"));
        assertEquals(5, topK.get("limit"));

        // isFull
        assertTrue((Boolean) data.get("full"));

        // isFreeConnex
        assertTrue((Boolean) data.get("freeConnex"));
    }

    @Test
    public void testParseWithLimit() throws Exception {
        String ddl = "CREATE TABLE Graph\n" +
                "(\n" +
                "    src    INT,\n" +
                "    dst    INT,\n" +
                "    rating DECIMAL\n" +
                ")";
        String query = "SELECT S.dst                                     AS A,\n" +
                "       S.src                                     AS B,\n" +
                "       R.src                                     AS C,\n" +
                "       T.dst                                     AS D,\n" +
                "       U.dst                                     AS E,\n" +
                "       R.rating + S.rating + T.rating + U.rating AS total_rating\n" +
                "FROM graph R,\n" +
                "     graph S,\n" +
                "     graph T,\n" +
                "     graph U\n" +
                "WHERE R.dst = S.src\n" +
                "  AND R.src = T.src\n" +
                "  AND R.src = U.src\n" +
                "ORDER BY total_rating DESC limit 5";

        ParseQueryRequest request = new ParseQueryRequest();
        request.setDdl(ddl);
        request.setQuery(query);

        ObjectMapper objectMapper = new ObjectMapper();
        String body = objectMapper.writeValueAsString(request);

        String response = mvc.perform(MockMvcRequestBuilders
                        .post("http://localhost:8848/api/v1/parse?orderBy=fanout&desc=false&limit=1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andReturn().getResponse().getContentAsString();
        Result result = objectMapper.readValue(response, Result.class);

        assertEquals(200, result.getCode());
        Map<String, Object> data = (Map<String, Object>) result.getData();

        // tables
        List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
        assertEquals(1, tables.size());
        assertEquals("Graph", tables.get(0).get("name"));
        assertEquals(3, ((List<String>) tables.get(0).get("columns")).size());
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("src"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("dst"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("rating"));

        // joinTrees
        // limit = 1
        assertEquals(1, ((List<Map<String, Object>>) data.get("joinTrees")).size());
        // desc is false
        assertEquals(1, ((List<Map<String, Object>>) data.get("joinTrees")).get(0).get("maxFanout"));
    }

    @Test
    public void testParseWithoutLimit() throws Exception {
        String ddl = "CREATE TABLE Graph\n" +
                "(\n" +
                "    src    INT,\n" +
                "    dst    INT,\n" +
                "    rating DECIMAL\n" +
                ")";
        String query = "SELECT S.dst                                     AS A,\n" +
                "       S.src                                     AS B,\n" +
                "       R.src                                     AS C,\n" +
                "       T.dst                                     AS D,\n" +
                "       U.dst                                     AS E,\n" +
                "       R.rating + S.rating + T.rating + U.rating AS total_rating\n" +
                "FROM graph R,\n" +
                "     graph S,\n" +
                "     graph T,\n" +
                "     graph U\n" +
                "WHERE R.dst = S.src\n" +
                "  AND R.src = T.src\n" +
                "  AND R.src = U.src\n" +
                "ORDER BY total_rating DESC limit 5";

        ParseQueryRequest request = new ParseQueryRequest();
        request.setDdl(ddl);
        request.setQuery(query);

        ObjectMapper objectMapper = new ObjectMapper();
        String body = objectMapper.writeValueAsString(request);

        String response = mvc.perform(MockMvcRequestBuilders
                        .post("http://localhost:8848/api/v1/parse?orderBy=fanout")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andReturn().getResponse().getContentAsString();
        Result result = objectMapper.readValue(response, Result.class);

        assertEquals(200, result.getCode());
        Map<String, Object> data = (Map<String, Object>) result.getData();

        // tables
        List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
        assertEquals(1, tables.size());
        assertEquals("Graph", tables.get(0).get("name"));
        assertEquals(3, ((List<String>) tables.get(0).get("columns")).size());
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("src"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("dst"));
        assertTrue(((List<String>) tables.get(0).get("columns")).contains("rating"));

        // joinTrees
        // no limit
        assertEquals(12, ((List<Map<String, Object>>) data.get("joinTrees")).size());
        // desc is false by default
        assertEquals(1, ((List<Map<String, Object>>) data.get("joinTrees")).get(0).get("maxFanout"));
    }
}
