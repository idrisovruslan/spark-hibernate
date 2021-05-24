package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.dmds.HubAcctTable;
import ru.idrisov.universal_loader.NewUniversalProcessor;

@Service
@AllArgsConstructor
public class HubAcctProcessor implements DatamartProcessor {

    NewUniversalProcessor newUniversalProcessor;
    HubAcctTable hubAcctTable;

    @Override
    public void process() {
        newUniversalProcessor.fillDfAndSaveToTable(hubAcctTable);
    }

    @Override
    public void init(String[] args) {

    }
}
