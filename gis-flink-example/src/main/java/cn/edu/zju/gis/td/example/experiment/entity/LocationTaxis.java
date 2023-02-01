package cn.edu.zju.gis.td.example.experiment.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedList;

/**
 * 某位置时间窗口内的出租车事件缓存
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-07
 */
@Deprecated
public class LocationTaxis {
    private final long locationId;
    private final double initialSpeed;
    private final Deque<TaxiEvent> taxiDeque;
    private double totalSpeed;
    private long totalFlow;

    public LocationTaxis(long locationId, double initialSpeed) {
        this.locationId = locationId;
        this.initialSpeed = initialSpeed;
        this.taxiDeque = new LinkedList<>();
        this.totalSpeed = 0.0;
        this.totalFlow = 0L;
    }

    public LocationTaxis(ResultSet rs) throws SQLException {
        this(rs.getLong("id"), rs.getDouble("init_velocity"));
    }

    public long getLocationId() {
        return locationId;
    }

    public void addEvent(TaxiEvent event) {
        // 对于停车等待的情况不进行去重
        this.taxiDeque.addLast(event);
        this.totalSpeed += event.getSpeed();
        this.totalFlow += event.getFlow();
        this.totalSpeed = Math.max(0.0, totalSpeed);
    }

    public void expire(long currentTime, long interval) {
        long leftBound = currentTime - interval;
        while (!taxiDeque.isEmpty() && taxiDeque.peekFirst().getTimestamp() < leftBound) {
            this.totalSpeed -= taxiDeque.peekFirst().getSpeed();
            this.totalSpeed = Math.max(0.0, totalSpeed);
            this.totalFlow -= taxiDeque.peekFirst().getFlow();
            this.totalFlow = Math.max(0L, totalFlow);
            taxiDeque.pollFirst();
        }
    }

    public double obtainFlowCount() {
        return totalFlow;
    }

    public double obtainAvgSpeed() {
        if (taxiDeque.isEmpty()) {
            return initialSpeed;
        }
        return totalSpeed / taxiDeque.size();
    }
}
