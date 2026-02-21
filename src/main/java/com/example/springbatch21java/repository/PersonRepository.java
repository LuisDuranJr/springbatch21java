package com.example.springbatch21java.repository;

import com.example.springbatch21java.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonRepository extends JpaRepository<Person, Integer> {
}
