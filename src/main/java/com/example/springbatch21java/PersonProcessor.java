package com.example.springbatch21java;

import net.bytebuddy.implementation.bytecode.Throw;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.springframework.batch.item.ItemProcessor;
import com.example.springbatch21java.model.Person;

import java.util.Arrays;
import java.util.List;

public class PersonProcessor implements ItemProcessor<Person, Person> {

    @Override
    public Person process(Person person) {

        /*if (person.getFirstName() == null || person.getFirstName().isEmpty()) {
            person.setFirstName(null); // force null
        }*/
        List<String> names = Arrays.asList("Allison","luis",null);
        for (String name : names){
            System.out.println("this is in the loop");
            if (name == null){
                System.out.println("this is null ");
                throw new IllegalArgumentException("Null value found");
            }
            System.out.println(name.length());
            if(name.isEmpty()){
                System.out.println("this is  empty");
            }

        }

        person.setFirstName(person.getFirstName().toUpperCase());
        person.setLastName(person.getLastName().toUpperCase());
        return person;
    }
}
