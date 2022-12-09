package fdu.daslab.message;

import java.io.Serializable;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/9 3:09 PM
 */

public class LaunchedExecutor implements Serializable {
    int execId;

    public LaunchedExecutor(int execId) {
        this.execId = execId;
    }
}
