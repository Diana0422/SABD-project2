package com.diagiac.flink.query2.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocationTemperature {
    private Double avgTemperature;
    private Long location;
}
