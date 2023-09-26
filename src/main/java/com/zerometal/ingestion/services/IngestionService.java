package com.zerometal.ingestion.services;

import static java.time.temporal.ChronoUnit.MILLIS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.zerometal.ingestion.dto.ExtractDTO;

@Service
public class IngestionService {

	private static final Logger LOGGER = LogManager.getLogger(IngestionService.class);
	
	@Value("${spring.datasource.url}")
	private String url;
	@Value("${spring.datasource.username}")
	private String username;	
	@Value("${spring.datasource.password}")
	private String password;
	
	public void ingest(String schema, String table) throws Exception {
		LOGGER.info(MessageFormat.format("Schema: {0} Table: {1}", schema, table));
		final LocalTime init = LocalTime.now();
		
		this.extract(ExtractDTO.builder().schema(schema).table(table).build());
		
		final LocalTime end = LocalTime.now();
		LOGGER.info(MessageFormat.format("Duration Total {0} seconds", MILLIS.between(init, end)/(double)1000));
	}

	private void extract(ExtractDTO extractDTO) {
		final LocalTime init = LocalTime.now();
		final int fetchSize = 500;
		final String fileName = MessageFormat.format("results/extract/{0}_{1}.csv", extractDTO.getSchema(), extractDTO.getTable());
		final Path fileNamePath = Paths.get(fileName);
		try {
			Files.deleteIfExists(fileNamePath);
		} catch (final IOException e) {
			e.printStackTrace();
		}
		
		//JDBC Native
		ResultSet resultSet = null;
		try (Connection connection = DriverManager.getConnection(this.url, this.username, this.password);
				Statement statement = connection.createStatement()) {
			
			connection.setAutoCommit(false); //For PostgreSQL
			statement.setFetchSize(fetchSize);
			
			resultSet = statement.executeQuery(MessageFormat.format("SELECT * FROM {0}.{1} LIMIT 5000", extractDTO.getSchema(), extractDTO.getTable()));
			final int columnSize = resultSet.getMetaData().getColumnCount();
			List<String> row;
			final List<String> tablePart = new ArrayList<>();
			int countRows = 0;
			while (resultSet.next()) {
				row = new ArrayList<>();
				for ( int i=1; i<=columnSize; i++ ) {
					row.add(resultSet.getObject(i)!=null ? resultSet.getObject(i).toString() : "");
				}
				
				tablePart.add(StringUtils.join(row, ","));
				
				if ( tablePart.size() == fetchSize ) {
					this.sendToFile(fileNamePath, tablePart);
				}
				
				if ( (++countRows % 50000) == 0 ) {
					LOGGER.info(MessageFormat.format("Rows: {0}", countRows));
				}
			}
			
			if ( tablePart!=null && !tablePart.isEmpty() ) {
				this.sendToFile(fileNamePath, tablePart);
			}
			
		} catch (final SQLException e) {
			LOGGER.error(e);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (final SQLException e) {
					LOGGER.error(e);
				}
			}
		}
		
		LOGGER.info(MessageFormat.format("Duration extract {0} seconds", MILLIS.between(init, LocalTime.now())/(double)1000));
	}
	
	private void sendToFile(Path fileNamePath, List<String> tablePart) {
		if ( tablePart!=null && !tablePart.isEmpty() ) {
			for (final String row : tablePart) {
				try {
					Files.write(fileNamePath, row.concat("\n").getBytes(), Files.exists(fileNamePath) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE_NEW);
				} catch (final IOException e) {
					e.printStackTrace();
				}
			}
			tablePart.clear();
		}
	}
	
}
