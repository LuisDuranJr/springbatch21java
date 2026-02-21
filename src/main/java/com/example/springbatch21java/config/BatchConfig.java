package com.example.springbatch21java.config;

import com.example.springbatch21java.PersonProcessor;
import com.example.springbatch21java.listener.PersonErrorListener;
import com.example.springbatch21java.listener.PersonSkipListener;
import com.example.springbatch21java.model.Person;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import org.hibernate.exception.DataException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataIntegrityViolationException;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("input.csv"))
                .delimited()
                .names("id","firstName","lastName","email")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Person.class);
                }})
                .linesToSkip(1)
                .build();
    }

    @Bean
    public JpaItemWriter<Person> writer(EntityManagerFactory emf) {
        JpaItemWriter<Person> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        return writer;
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, FlatFileItemReader<Person> reader, JpaItemWriter<Person> writer, PersonErrorListener errorListener, PersonSkipListener personSkipListener) {
        return stepBuilderFactory.get("step")
                .<Person, Person>chunk(5)
                .reader(reader)
                .processor(new PersonProcessor())  // Add processor here
                .writer(writer)
                //.listener(errorListener)
                .faultTolerant()
                //.skip(PersistenceException.class)
                //.skip(NullPointerException.class)
                .skip(IllegalArgumentException.class)
                .listener(personSkipListener)// skip NPEs to continue job
                .skipLimit(100)
                .build();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("importPersonJob")
                .start(step)
                .build();
    }
}
