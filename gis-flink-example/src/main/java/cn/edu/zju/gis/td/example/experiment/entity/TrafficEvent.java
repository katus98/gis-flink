package cn.edu.zju.gis.td.example.experiment.entity;

/**
 * @author SUN Katus
 * @version 1.0, 2023-01-30
 */
public interface TrafficEvent {
    long getId();
    long getTimestamp();
    String getAddress();
    double getLon();
    double getLat();
    void setUnitId(long unitId);

    default boolean checkAddressContains(String... sites) {
        for (String site : sites) {
            if (!site.isEmpty() && getAddress().contains(site)) {
                return true;
            }
        }
        return false;
    }
}
