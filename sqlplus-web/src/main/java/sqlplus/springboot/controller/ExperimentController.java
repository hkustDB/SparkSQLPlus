package sqlplus.springboot.controller;

import sqlplus.springboot.component.ExperimentManager;
import sqlplus.springboot.dto.ExperimentStartRequest;
import sqlplus.springboot.dto.ExperimentStatusResponse;
import sqlplus.springboot.dto.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import sqlplus.springboot.util.CustomQueryManager;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class ExperimentController {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExperimentController.class);

    private final ExperimentManager manager;

    @Autowired
    public ExperimentController(ExperimentManager manager) {
        this.manager = manager;
    }

    @GetMapping("/experiment/status")
    public Result status() {
        return mkStatusResult();
    }

    @PostMapping("/experiment/start")
    public Result start(@RequestBody ExperimentStartRequest request) {
        Result result = new Result();
        result.setCode(200);
        manager.start(request.getExperiments().stream().sorted().collect(Collectors.toList()));

        return result;
    }

    @PostMapping("/experiment/stop")
    public Result stop() {
        Result result = new Result();
        result.setCode(200);
        manager.stop();

        return result;
    }

    @GetMapping("/experiment/queries")
    public Result queries() {
        Result result = new Result();
        result.setCode(200);
        String[] builtinQueries = new String[]{"Query1", "Query2", "Query3", "Query4", "Query5", "Query6", "Query7", "Query8"};
        List<String> customQueries = CustomQueryManager.list("examples/query/custom/");

        List<String> allQueries = Arrays.stream(builtinQueries).collect(Collectors.toList());
        allQueries.addAll(customQueries);
        result.setData(allQueries);

        return result;
    }

    private Result mkStatusResult() {
        Result result = new Result();
        result.setCode(200);
        ExperimentStatusResponse response = new ExperimentStatusResponse();
        manager.fillExperimentStatusResponse(response);
        result.setData(response);
        return result;
    }
}