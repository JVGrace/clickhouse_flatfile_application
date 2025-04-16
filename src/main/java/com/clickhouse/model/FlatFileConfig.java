package com.clickhouse.model;

import lombok.Data;

@Data
public class FlatFileConfig {
	    private String delimiter;  // e.g. ","
	    public String getDelimiter() {
	        return delimiter;
	    }

	    public void setDelimiter(String delimiter) {
	        this.delimiter = delimiter;
	    }

}
