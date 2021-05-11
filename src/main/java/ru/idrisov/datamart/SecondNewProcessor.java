package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.datamart.universal_loader.NewUniversalProcessor;
import ru.idrisov.domain.entitys.tests.SecondTargetTable;

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
