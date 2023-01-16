package cn.edu.zju.gis.td.example.experiment.entity;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-16
 */
public interface TextSerializable {
    long getId();
    long getTimestamp();
    byte[] toByteArray();
}
