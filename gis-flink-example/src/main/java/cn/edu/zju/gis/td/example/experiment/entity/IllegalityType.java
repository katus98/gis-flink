package cn.edu.zju.gis.td.example.experiment.entity;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 交通违法类型
 *
 * @author SUN Katus
 * @version 1.0, 2023-01-30
 */
public enum IllegalityType {
    SCRAMBLE("争抢类交通违法"),
    BEHAVIOR("影响驾驶行为违法"),
    REVERSE("逆向行驶违法"),
    OVER_SPEED("超速违法"),
    SIGNALS("违反交通信号违法"),
    OTHERS("其他违法");

    IllegalityType(String name) {
        this.name = name;
    }

    @Getter
    private final String name;

    public static IllegalityType parseFrom(String type) {
        switch (type) {
            case "未按规定让行":
            case "未低速通过":
            case "违法变更车道":
            case "违法超车":
            case "违法抢行":
                return SCRAMBLE;
            case "酒后驾驶":
            case "疲劳驾驶":
                return BEHAVIOR;
            case "逆向行驶":
                return REVERSE;
            case "超速行驶":
                return OVER_SPEED;
            case "违反交通信号":
                return SIGNALS;
            default:
                return OTHERS;
        }
    }

    public static final List<IllegalityType> ALL_TYPES = Collections.unmodifiableList(Arrays.asList(SCRAMBLE, BEHAVIOR, REVERSE, OVER_SPEED, SIGNALS, OTHERS));
}
