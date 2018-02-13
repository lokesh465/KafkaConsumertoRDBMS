package com.jnit.kafka.dao;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.transaction.annotation.Transactional;

import com.jnit.model.DeviceDB;


public interface DeviceRepository extends CrudRepository<DeviceDB, Integer> {

	@Transactional(readOnly = true)
	List<DeviceDB> findByAccNumber(String accNumber);
	

}
