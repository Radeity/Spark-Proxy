package fdu.daslab;

/**
 * @author Aaron Wang
 * @date 2022/11/30 20:37 PM
 * @version 1.0
 */
public class YarnClientAspectMocTest {
//    protected static final Logger logger = LoggerFactory.getLogger(YarnClientAspectMocTest.class);
//
//    private final PrintStream standardOut = System.out;
//    ByteArrayOutputStream stdoutStream = new ByteArrayOutputStream();
//    @Before
//    public void beforeEveryTest(){
//        System.setOut(new PrintStream(stdoutStream));
//    }
//    @After
//    public void afterEveryTest() throws IOException {
//        System.setOut(standardOut);
//        stdoutStream.close();
//    }
//    @Test
//    public void testMoc(){
//        YarnClientMoc moc = new YarnClientMoc();
//        try {
//
//            ApplicationSubmissionContext appContext = ApplicationSubmissionContext.newInstance(
//                    ApplicationId.newInstance(System.currentTimeMillis(),1236),"appName",
//                    "queue", Priority.UNDEFINED,
//                    null,false,
//                    false,10,null,
//                    "type");
//            moc.createAppId();
//            ApplicationId applicationId = moc.submitApplication(appContext);
//            String stdoutContent = stdoutStream.toString();
//            Assert.assertTrue("Do not capture log output!", stdoutContent.contains("create"));
//            Assert.assertTrue("trigger YarnClientAspectMoc.submitApplication failed"
//                    ,stdoutContent.contains("YarnClientAspectMoc[submitApplication]"));
////            Assert.assertTrue("trigger YarnClientAspectMoc.createAppId failed",
////                    stdoutContent.contains("YarnClientAspectMoc[createAppId]:"));
////            throw new IOException("ioioioioioio");
//        } catch (YarnException | IOException e) {
//            logger.error("test YarnClientAspectMoc failed", e);
////            Assert.fail("test YarnClientAspectMoc failed: "+e.getMessage());
//            e.printStackTrace();
//        }
//    }
}
