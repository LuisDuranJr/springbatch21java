package com.example.springbatch21java.repository;

import com.example.springbatch21java.model.PersonError;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonErrorRepository extends JpaRepository<PersonError, Long> {
}
