package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author SUN Katus
 * @version 1.0, 2022-11-30
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TempPoint {
    private double x;
    private double y;

    @Override
    public String toString() {
        return "(" + x + ", " + y + ')';
    }
}
