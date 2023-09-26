package com.zerometal.ingestion.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ExtractDTO {
	private String schema;
	private String table;
}
