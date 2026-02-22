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
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    /*@Bean
    public SynchronizedItemStreamReader<Person> reader() {
        // 1. Define the actual file reader logic
        FlatFileItemReader<Person> delegate = new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("input.csv"))
                .saveState(false)
                .delimited()
                .names("id", "firstName", "lastName", "email")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .linesToSkip(1)
                .build();

        // 2. Wrap it to make it thread-safe
        return new SynchronizedItemStreamReaderBuilder<Person>()
                .delegate(delegate)
                .build();
    }*/
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("input.csv"))
                .saveState(false)
                .delimited()
                .names("id", "firstName", "lastName", "email")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
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
    public Step step(StepBuilderFactory stepBuilderFactory, FlatFileItemReader<Person> reader, JpaItemWriter<Person> writer, PersonErrorListener errorListener, PersonSkipListener personSkipListener,@Qualifier("myCustomExecutor")TaskExecutor taskExecutor) {
        return stepBuilderFactory.get("step")
                .<Person, Person>chunk(10)
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
                .taskExecutor(taskExecutor)
                .throttleLimit(10)
                .build();
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("importPersonJob")
                .start(step)
                .build();
    }
    @Bean(name = "myCustomExecutor")
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("batch-thread-");
        // 2. Set the handler to log the rejection
       // executor.setRejectedExecutionHandler((runnable, exec) -> {
            // This is where you can force a log message
         //   System.err.println("CRITICAL: Task rejected! Thread pool and queue are full.");
      //  });
        // Use this to stop data loss and slow down the reader
        //executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // This will force the exception to "fire"
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
