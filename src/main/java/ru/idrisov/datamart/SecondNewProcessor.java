package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.SecondTargetTable;

@Service
@AllArgsConstructor
public class SecondNewProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    SecondTargetTable secondTargetTable;

    @Override
    public void process() {
        newUniversalProcessor.fillTable(secondTargetTable);
    }

    @Override
    public void init(String[] args) {

    }
}
