package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.tests.SecondTargetTable;
import ru.idrisov.universal_loader.NewUniversalProcessor;

@Service
@AllArgsConstructor
public class SecondNewProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    SecondTargetTable secondTargetTable;

    @Override
    public void process() {
        newUniversalProcessor.fillDfAndSaveToTable(secondTargetTable);
    }

    @Override
    public void init(String[] args) {

    }
}
