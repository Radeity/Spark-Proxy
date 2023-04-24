package fdu.daslab.dispatcher;

import fdu.daslab.dispatcher.runner.SchedulerServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

/**
 * @author Aaron Wang
 * @version 1.0
 * @date 2022/12/8 4:29 PM
 */
@SpringBootApplication
public class Dispatcher {

    @Autowired
    private SchedulerServer schedulerServer;

    public static void main(String[] args) {
        SpringApplication.run(Dispatcher.class);
    }

    @PostConstruct
    public void run() {
        this.schedulerServer.run();
    }
}
