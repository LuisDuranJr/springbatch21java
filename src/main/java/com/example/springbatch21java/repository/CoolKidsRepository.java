package com.example.springbatch21java.repository;

import com.example.springbatch21java.model.CoolKids;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CoolKidsRepository extends JpaRepository<CoolKids, Long> {
}
