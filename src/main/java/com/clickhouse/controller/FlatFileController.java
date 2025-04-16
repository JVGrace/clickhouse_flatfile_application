package com.clickhouse.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.clickhouse.model.ColumnMeta;
import com.clickhouse.model.FlatFileConfig;
import com.clickhouse.service.FlatFileService;

@RestController
@RequestMapping("/api/flatfile")
@CrossOrigin(origins = "*")
public class FlatFileController {

    @Autowired
    private FlatFileService flatFileService;
    
    @GetMapping
    public String status() {
        return "Flat File API is up!";
    }

    @PostMapping("/schema")
    public List<ColumnMeta> getFlatFileSchema(@RequestParam("file") MultipartFile file,
                                              @RequestParam("delimiter") String delimiter) throws Exception {
        FlatFileConfig config = new FlatFileConfig();
        config.setDelimiter(delimiter);
        return flatFileService.extractSchema(file, config);
    }

    @PostMapping("/count")
    public int countRecords(@RequestParam("file") MultipartFile file,
                            @RequestParam("delimiter") String delimiter) throws Exception {
        FlatFileConfig config = new FlatFileConfig();
        config.setDelimiter(delimiter);
        return flatFileService.countRecords(file, config);
    }
}
