package com.example.springbatch21java.listener;

import com.example.springbatch21java.model.Person;
import com.example.springbatch21java.model.PersonError;
import com.example.springbatch21java.repository.PersonErrorRepository;
import org.springframework.batch.core.SkipListener;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PersonSkipListener implements SkipListener<Person, Person> {

    private final PersonErrorRepository errorRepository;

    public PersonSkipListener(PersonErrorRepository errorRepository) {
        this.errorRepository = errorRepository;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        // optional
    }

    @Override
    public void onSkipInProcess(Person item, Throwable t) {
        System.out.println("this is the skip in process");
        System.out.println("This is what was skipped " + item.getFirstName());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void onSkipInWrite(Person item, Throwable t) {
        System.out.println("this is the skip in write");
        String rootMessage = NestedExceptionUtils.getMostSpecificCause(t).getMessage();
        PersonError error = new PersonError();
        error.setPersonId(item.getId());
        error.setErrorMessage(rootMessage);
        String fullStack = getStackTraceAsString(t);
        error.setStackTrace(fullStack.substring(0, Math.min(255, fullStack.length())));
        errorRepository.save(error);
    }

    private String getStackTraceAsString(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement elem : t.getStackTrace()) {
            sb.append(elem.toString()).append("\n");
        }
        return sb.toString();
    }
}
