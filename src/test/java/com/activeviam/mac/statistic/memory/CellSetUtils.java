package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.Assertions;

public class CellSetUtils {

  public static Long extractValueFromSingleCellDTO(CellSetDTO data) {
    Assertions.assertThat(data.getCells()).hasSize(1);

    String sum_s = data.getCells().iterator().next().toString();
    String[] cell = sum_s.split(",");
    Long value = null;
    for (String attr : cell) {
      if (attr.contains(" value=")) {
        value = Long.parseLong(attr.replace(" value=", ""));
      }
    }
    return value;
  }

  public static Double extractDoubleValueFromSingleCellDTO(CellSetDTO data) {
    Assertions.assertThat(data.getCells()).hasSize(1);

    String sum_s = data.getCells().iterator().next().toString();
    String[] cell = sum_s.split(",");
    Double value = null;
    for (String attr : cell) {
      if (attr.contains(" value=")) {
        value = Double.parseDouble(attr.replace(" value=", ""));
      }
    }
    return value;
  }

  public static Double[] extractValuesFromCellSetDTO(CellSetDTO data) {
    final AtomicInteger cursor = new AtomicInteger();
    Double[] res = new Double[data.getCells().size()];
    data.getCells()
        .forEach(
            cell -> {
              int i = cursor.getAndIncrement();
              String[] cell_s = cell.toString().split(",");
              for (String attr : cell_s) {

                if (attr.contains(" value=")) {
                  res[i] = Double.parseDouble(attr.replace(" value=", ""));
                }
              }
            });
    return res;
  }

  public static Long sumValuesFromCellSetDTO(CellSetDTO data) {
    final AtomicLong value = new AtomicLong();
    data.getCells()
        .forEach(
            cell -> {
              String[] cell_s = cell.toString().split(",");

              for (String attr : cell_s) {
                if (attr.contains(" value=")) {
                  value.addAndGet(Long.parseLong(attr.replace(" value=", "")));
                }
              }
            });
    return value.get();
  }
}
