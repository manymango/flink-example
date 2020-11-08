package org.manymango.example.model;

/**
 * @author lcx
 * @date 2020/11/8 18:06
 */
public class Event {

    private String name;

    /**
     * 事件类型
     */
    private int type;

    public Event() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
