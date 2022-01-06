package com.mahfooz.spark.streaming.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class DeviceData {

    private String device;
    private String type;
    private Double signal;
    private java.sql.Date time;

}
