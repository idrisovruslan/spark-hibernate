package ru.idrisov.universal_loader;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Getter
public class CycleValue {
    //TODO КОмпановщик(рекурсивно)
    final String mainCycleValue;
    final List<CycleValue> nestedCycleValue;
}
