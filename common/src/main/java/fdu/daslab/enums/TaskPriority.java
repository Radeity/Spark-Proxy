package fdu.daslab.enums;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 4/23/23 6:11 AM
 */
// TODO: Future work, set task priority in scheduler strategy
public enum TaskPriority {
    HIGH(0),
    MEDIUM(1),
    LOW(2);

    private final int code;

    TaskPriority(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

}
