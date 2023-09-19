package com.zerometal.ingestion.web;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.zerometal.ingestion.services.IngestionService;

@RestController
@RequestMapping(path = "/", produces = MediaType.APPLICATION_JSON_VALUE)
public class IngestionController {

	private static final Logger LOGGER = LogManager.getLogger(IngestionController.class);
	
	@Autowired
	private IngestionService service;
	
	@GetMapping(path = "/{schema}/{table}")
	public ResponseEntity<Object> execute(@PathVariable("schema") String schema, @PathVariable("table") String table) {
		final Object response = null;
		HttpStatus status = HttpStatus.OK;
		
		try {
			this.service.ingest(schema, table);
		} catch (final Exception e) {
			LOGGER.info(e.getMessage());
			status = HttpStatus.INTERNAL_SERVER_ERROR;
		}

		return ResponseEntity.status(status).body(response);
	}
	
}
