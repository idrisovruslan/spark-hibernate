package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.tests.ThirdlyTargetTable;
import ru.idrisov.universal_loader.NewUniversalProcessor;

@Service
@AllArgsConstructor
public class ThirdlyNewProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    ThirdlyTargetTable thirdlyTargetTable;

    @Override
    public void process() {
        newUniversalProcessor.fillDfAndSaveToTable(thirdlyTargetTable);
    }

    @Override
    public void init(String[] args) {

    }
}
