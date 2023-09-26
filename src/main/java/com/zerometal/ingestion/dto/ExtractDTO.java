package com.zerometal.ingestion.dto;

import java.nio.file.Path;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ExtractDTO {
	private String schema;
	private String table;
	private Path outputFile;
}
