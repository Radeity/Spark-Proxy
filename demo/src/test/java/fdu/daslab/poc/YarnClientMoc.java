package fdu.daslab.poc;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @author Aaron Wang
 * @date 2022/11/30 20:16 PM
 * @version 1.0
 */
public class YarnClientMoc {
    protected static final Logger logger = LoggerFactory.getLogger(YarnClientMoc.class);

    private Random random = new Random();

    public ApplicationId createAppId() {
        ApplicationId created = ApplicationId.newInstance(System.currentTimeMillis(), random.nextInt());
//        System.out.println("created id "+created.getId());
//        logger.info("created id {}", created.getId());
        return created;
    }

    public ApplicationId
    submitApplication(ApplicationSubmissionContext appContext)
            throws YarnException, IOException {
        ApplicationId appId = createAppId();
//        logger.info("created id {}", appId.getId());
        logger.info("YarnClientAspectMoc[submitApplication]: app context: {}, submittedAppId: {}", appContext, appId);
        return appId;
    }
}
