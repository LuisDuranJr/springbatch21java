package com.example.springbatch21java;

import com.example.springbatch21java.model.CoolKids;
import com.example.springbatch21java.repository.CoolKidsRepository;
import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import com.example.springbatch21java.model.Person;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersonProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonProcessor.class);

    private final CoolKidsRepository coolKidsRepository;

    public PersonProcessor(CoolKidsRepository coolKidsRepository) {
        this.coolKidsRepository = coolKidsRepository;
    }
    @Override
    public Person process(Person person) throws InterruptedException {
        log.info("Thread: {} | Processing item: {}", Thread.currentThread().getName(), person.toString());


        if (person.getFirstName().equalsIgnoreCase("Luis") || person.getFirstName().equalsIgnoreCase("Mike")) {

            CoolKids coolkid = new CoolKids();
            coolkid.setFirstName(person.getFirstName());
            coolkid.setLastName(person.getLastName());
            coolkid.setEmail(person.getEmail());
            coolKidsRepository.save(coolkid);

            return null;

        } else {
            Thread.sleep(1000);
            person.setFirstName(person.getFirstName().toUpperCase());
            person.setLastName(person.getLastName().toUpperCase());
            return person;
        }
    }
}
