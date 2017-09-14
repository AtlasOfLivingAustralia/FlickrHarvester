package au.org.ala.flickrharvester;

import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.photos.PhotoList;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.CsvDataFormat;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class FlickrHarvester {
    private static final Logger log = LoggerFactory.getLogger(FlickrHarvester.class);


    private static final String DEFAULT_CONFIG_FILE = "config.properties";
    private static final String DEFAULT_THREAD_COUNT = "3";
    private static final String DEFAULT_OUTPUT_DIR = "/tmp/";
    private static final String DEFAULT_OUTPUT_FILE = "flickr.dwc.csv";
    private final Config config;
    private final String outputDir;
    private final String outputFile;

    public FlickrHarvester(String configFile, String dir, String file) throws ConfigurationException, java.text.ParseException {
        this.config = new Config(configFile);
        this.outputDir = dir;
        this.outputFile = file;
    }

    private void run(int threads) throws Exception {
        final BrokerService broker = new BrokerService();
        final String brokerUrl = "tcp://localhost:61616";
        // configure the broker
        broker.addConnector(brokerUrl);
        broker.setUseJmx(true);
        broker.setPersistent(false);
        broker.start();


        ConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(brokerUrl);

        CamelContext context = new DefaultCamelContext();
        context.addComponent("amq",
                JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

        context.addRoutes(new RouteBuilder() {
            public void configure() {
                // Set up the route — from the input JMS broker, to
                //  the output directory, via a log operation
                // Note that we're specifying the log class as JMSToFile, and
                //  the log level as DEBUG. We will use these parameters in
                //  the log configuration file log4j.properties to control
                //  logging from the program.
                // Note also that the specific destination we consume from is
                //  test_queue. To exercise this program, we must place messages
                //  in that destination using a JMS client

                from("amq:reuests.backlog").throttle(10).timePeriodMillis(1000 * 60)
                        .to("amq:reuests.todo")
                ;

                CsvDataFormat csvDataFormat = new CsvDataFormat();
                csvDataFormat.setFormatName(null);
                csvDataFormat.setDelimiter(",");
                csvDataFormat.setQuote("\"");
                csvDataFormat.setHeader(config.CSV_FIELD_LIST);
                csvDataFormat.setSkipHeaderRecord(true);
                csvDataFormat.setEscape("\\");
                csvDataFormat.setEscapeDisabled(false);

                from("amq:output.csv")
                        .marshal(csvDataFormat)
                        .to("file:" + outputDir + "?fileName=" + outputFile + "&fileExist=Append");

            }
        });
        context.start();

        // Create a Connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue("reuests.backlog");

        // Create a MessageProducer from the Session to the Topic or Queue
        MessageProducer producer = session.createProducer(destination);


        Flickr flickr = new Flickr(config.API_KEY, config.SHARED_SECRET, new REST());
        PhotoList photoList = flickr.getPoolsInterface().getPhotos(config.GROUP_ID, config.USER_ID, config.MACHINE_TAGS, new HashSet<String>(Arrays.asList(config.EXTRAS)),
                config.PER_PAGE, 0);
        log.info("Total number of photos:{}, Number of pages in total:{}, Number of photos per page:{}", photoList.getTotal(), photoList.getPages(), photoList.getPerPage());
        for (int page = 1; page <= photoList.getPages(); page++) {
//        for (int page = 1; page <= 5; page++) {

            ObjectMessage message = session.createObjectMessage(new FlickrRequest(config.GROUP_ID, config.USER_ID, config.MACHINE_TAGS, config.EXTRAS, config.PER_PAGE, page,
                    null, null, FlickrRequest.RequestType.POOL_SEARCH));
            log.info("Request scheduled: " + message.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(message);
        }
        for (int i = 0; i < threads; i++) {
            producer.send(session.createObjectMessage(new FlickrRequest(null, null, null, null, 0, 0,
                    null, null, FlickrRequest.RequestType.END_OF_OPERATION)));
        }

        producer.close();
        session.close();
        connection.close();

        final ExecutorService flickrExecutor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            flickrExecutor.execute(new FlickrProcessor(config, context));
        }
        flickrExecutor.shutdown();
        int flickrExecutorWait = 0;
        while (!flickrExecutor.awaitTermination(1, TimeUnit.MINUTES) && !Thread.currentThread().isInterrupted()
                && flickrExecutorWait < 600) {
            flickrExecutorWait++;
            log.warn("Threads not complete after {} minutes, waiting for them again", flickrExecutorWait);
        }
        context.stop();
        broker.stop();
        broker.waitUntilStopped();
        flickrExecutor.shutdownNow();
        log.info("Executor service is shutdown now.");
    }

    public static void main(String... args) throws Exception {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", simpleDateFormat.toPattern());
        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "java.lang,javax.security,java.util,au.org.ala.flickrharvester");


        if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
        }

        final CommandLineParser parser = new DefaultParser();


        final Options options = new Options();
        options.addOption("h", "help", false, "prints this message.");
        options.addOption("c", "config", true, "Configuration file path. default is:" + DEFAULT_CONFIG_FILE);
        options.addOption("t", "threads", true, "Number of threads. default is:" + DEFAULT_THREAD_COUNT);
        options.addOption("od", "outputdir", true, "Output directory. default is:" + DEFAULT_OUTPUT_DIR);
        options.addOption("of", "outputfile", true, "Output File. default is:" + DEFAULT_OUTPUT_FILE);
        try {
            // parse the command line arguments
            final CommandLine line = parser.parse(options, args);

            if (line.hasOption("help")) {
                final HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp("FlickrHarvester", options);
                return;
            }
            final String cfg = line.getOptionValue("config", DEFAULT_CONFIG_FILE);
            final String dir = line.getOptionValue("outputdir", DEFAULT_OUTPUT_DIR);
            final String file = line.getOptionValue("outputfile", DEFAULT_OUTPUT_FILE);
            final int threads = Integer.parseInt(line.getOptionValue("thread", DEFAULT_THREAD_COUNT));
            Files.deleteIfExists(Paths.get(dir + file));
            FlickrHarvester harvester = new FlickrHarvester(cfg, dir, file);
            harvester.run(threads);


        } catch (ParseException exp) {
            System.out.println("Unexpected exception: " + exp.getMessage());
            final HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("FlickrHarvester", options);
            throw exp;
        }
    }


}
