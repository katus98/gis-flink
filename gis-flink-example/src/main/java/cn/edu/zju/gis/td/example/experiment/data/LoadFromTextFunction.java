package cn.edu.zju.gis.td.example.experiment.data;

import cn.edu.zju.gis.td.example.experiment.entity.TextSerializable;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-16
 */
@FunctionalInterface
public interface LoadFromTextFunction {
    TextSerializable load(String line);
}
