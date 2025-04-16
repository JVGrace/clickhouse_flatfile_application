package com.clickhouse.service;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.clickhouse.model.ColumnMeta;
import com.clickhouse.model.FlatFileConfig;

@Service
public class FlatFileService {

    public List<ColumnMeta> extractSchema(MultipartFile file, FlatFileConfig config) throws Exception {
        List<ColumnMeta> columns = new ArrayList<>();

        try (CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter(config.getDelimiter().charAt(0))
                .parse(new InputStreamReader(file.getInputStream()))) {

            for (String header : parser.getHeaderNames()) {
                columns.add(new ColumnMeta(header, "String")); // You can enhance type inference later
            }
        }

        return columns;
    }

    public int countRecords(MultipartFile file, FlatFileConfig config) throws Exception {
        try (CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter(config.getDelimiter().charAt(0))
                .parse(new InputStreamReader(file.getInputStream()))) {
            return parser.getRecords().size();
        }
    }
}
