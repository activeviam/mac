package com.activeviam.mac.statistic.memory;

import com.quartetfs.biz.pivot.dto.CellSetDTO;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;

public class CellSetUtils {
  @SuppressWarnings("unchecked")
  public static <T> T extractValueFromSingleCellDTO(CellSetDTO data) {
    Assertions.assertThat(data.getCells().size()).isEqualTo(1);
    return (T) data.getCells().get(0).getValue();
  }

  @SuppressWarnings("unchecked")
  public static <T> List<T> extractValuesFromCellSetDTO(CellSetDTO data) {
    return data.getCells().stream()
        .map(c -> (T) c.getValue())
        .collect(Collectors.toList());
  }
}
