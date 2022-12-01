package fdu.daslab.poc;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Aaron Wang
 * @date 2022/11/30 20:02 PM
 * @version 1.0
 */
@Aspect
public class YarnClientAspectMoc {
    protected static final Logger logger = LoggerFactory.getLogger(YarnClientAspectMoc.class);

    private ApplicationId privateId = null;
    @AfterReturning(pointcut="execution(ApplicationId fdu.daslab.poc.YarnClientMoc.submitApplication(ApplicationSubmissionContext)) && args(appContext)",
    returning = "submittedAppId",argNames = "appContext")
    public void submitApplication(ApplicationSubmissionContext appContext,ApplicationId submittedAppId) throws Throwable {
        logger.info("YarnClientAspectMoc[submitApplication]: app context: {}, submittedAppId: {}, privateId: {}", appContext, submittedAppId, privateId);
    }
    // 只有在submitApplication内部调用createAppId时拦截，获取createAppId的返回值
    @AfterReturning(pointcut="cflow(execution(ApplicationId fdu.daslab.poc.YarnClientMoc.submitApplication(ApplicationSubmissionContext))) " +
            "&& !within(YarnClientAspectMoc) && execution(ApplicationId fdu.daslab.poc.YarnClientMoc.createAppId())",
            returning = "submittedAppId")
    public void createAppId(ApplicationId submittedAppId) throws Throwable {
        privateId = submittedAppId;
        logger.info("YarnClientAspectMoc[createAppId]: created submittedAppId {}", submittedAppId);
    }
}
