package com.clickhouse.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.stereotype.Service;

import com.clickhouse.model.ClickHouseConfig;
import com.clickhouse.model.ColumnMeta;

@Service
public class ClickHouseService {

    public List<String> fetchTables(ClickHouseConfig config) throws SQLException {
        String url = buildJdbcUrl(config);
        Properties props = buildAuthProperties(config);

        try (Connection conn = DriverManager.getConnection(url, props);
             ResultSet rs = conn.getMetaData().getTables(config.getDatabase(), null, "%", new String[]{"TABLE"})) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
            return tables;
        }
    }

    public List<ColumnMeta> fetchColumns(ClickHouseConfig config, String tableName) throws SQLException {
        String url = buildJdbcUrl(config);
        Properties props = buildAuthProperties(config);

        try (Connection conn = DriverManager.getConnection(url, props);
             ResultSet rs = conn.getMetaData().getColumns(config.getDatabase(), null, tableName, "%")) {

            List<ColumnMeta> columns = new ArrayList<>();
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                columns.add(new ColumnMeta(name, type));
            }
            return columns;
        }
    }

    private String buildJdbcUrl(ClickHouseConfig config) {
        return String.format("jdbc:clickhouse://%s:%s/%s", config.getHost(), config.getPort(), config.getDatabase());
    }

    private Properties buildAuthProperties(ClickHouseConfig config) {
        Properties props = new Properties();
        props.setProperty("user", config.getUser());
        props.setProperty("custom_http_headers", "Authorization=Bearer " + config.getJwt());
        return props;
    }
}
