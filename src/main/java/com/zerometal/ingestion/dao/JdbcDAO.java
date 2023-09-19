package com.zerometal.ingestion.dao;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.Getter;

@Repository
public class JdbcDAO {
	
	@Getter
	private JdbcTemplate jdbcTemplate;
	
	@Autowired 
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource); 
    }
		
}
