package com.clickhouse.controller;

import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.clickhouse.model.ClickHouseConfig;
import com.clickhouse.model.ColumnMeta;
import com.clickhouse.service.ClickHouseService;

@RestController
@RequestMapping("/api/clickhouse")
@CrossOrigin(origins = "*")
public class ClickHouseContoller {

    @Autowired
    private ClickHouseService clickHouseService;
    @GetMapping
    public String status() {
        return "Click House File API is up!";
    }

    @PostMapping("/tables")
    public List<String> getTables(@RequestBody ClickHouseConfig config) throws SQLException {
        return clickHouseService.fetchTables(config);
    }

    @PostMapping("/columns")
    public List<ColumnMeta> getColumns(@RequestBody ClickHouseConfig config,
                                       @RequestParam String tableName) throws SQLException {
        return clickHouseService.fetchColumns(config, tableName);
    }
}
