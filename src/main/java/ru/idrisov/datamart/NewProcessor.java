package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.TargetTable;

@Service
@AllArgsConstructor
public class NewProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    TargetTable targetTable;

    @Override
    public void process() {
        newUniversalProcessor.fillTable(targetTable);
    }

    @Override
    public void init(String[] args) {

    }
}
