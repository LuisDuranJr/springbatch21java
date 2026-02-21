package com.example.springbatch21java.listener;

import com.example.springbatch21java.model.Person;
import com.example.springbatch21java.model.PersonError;
import com.example.springbatch21java.repository.PersonErrorRepository;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PersonErrorListener implements ItemProcessListener<Person, Person> {

    private final PersonErrorRepository errorRepository;

    public PersonErrorListener(PersonErrorRepository errorRepository) {
        this.errorRepository = errorRepository;
    }

    @Override
    public void beforeProcess(Person item) {
        // no-op
    }

    @Override
    public void afterProcess(Person item, Person result) {
        // no-op
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void onProcessError(Person item, Exception e) {
        PersonError error = new PersonError();
        error.setPersonId(item.getId());
        error.setErrorMessage(e.getMessage());
        //error.setStackTrace(getStackTraceAsString(e));
        String fullStack = getStackTraceAsString(e);
        error.setStackTrace(fullStack.substring(0, Math.min(255, fullStack.length())));
        errorRepository.save(error);
    }

    private String getStackTraceAsString(Exception e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement elem : e.getStackTrace()) {
            sb.append(elem.toString()).append("\n");
        }
        return sb.toString();
    }
}
