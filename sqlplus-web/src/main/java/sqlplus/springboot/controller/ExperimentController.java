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
        LOGGER.info(request.toString());
        request.getExperiments().stream().sorted().forEach(manager::addPendingExperiment);
        manager.start();

        return result;
    }

    private Result mkStatusResult() {
        Result result = new Result();
        result.setCode(200);
        ExperimentStatusResponse response = new ExperimentStatusResponse();
        response.setStatus(manager.getStatus().status);
        response.setExperimentNames(manager.getExperimentNames());
        response.setExperimentStatus(manager.getExperimentStatus());
        response.setExperimentResults(manager.getExperimentResults());
        result.setData(response);
        return result;
    }
}