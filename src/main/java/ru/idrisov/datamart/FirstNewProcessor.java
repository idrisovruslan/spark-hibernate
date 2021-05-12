package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.tests.FirstTargetTable;
import ru.idrisov.universal_loader.NewUniversalProcessor;

@Service
@AllArgsConstructor
public class FirstNewProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    FirstTargetTable firstTargetTable;

    @Override
    public void process() {
        newUniversalProcessor.fillTable(firstTargetTable);
    }

    @Override
    public void init(String[] args) {

    }
}
