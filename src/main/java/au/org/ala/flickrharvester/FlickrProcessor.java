package au.org.ala.flickrharvester;


import com.flickr4java.flickr.Flickr;
import com.flickr4java.flickr.FlickrException;
import com.flickr4java.flickr.REST;
import com.flickr4java.flickr.photos.Photo;
import com.flickr4java.flickr.photos.PhotoList;
import com.flickr4java.flickr.photos.licenses.License;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlickrProcessor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(FlickrProcessor.class);
    private final MessageConsumer consumer;
    private final Session session;
    private final Connection connection;
    private static final AtomicInteger requestCount = new AtomicInteger(0);
    private final Flickr flickr;
    private final Config config;
    private final Map<String, String> licenseMap = new HashMap<>();
    private final ProducerTemplate template;
    private final Destination destination;

    public FlickrProcessor(Config config, CamelContext context) throws JMSException, FlickrException {

        log.debug("Initialising FlickrProcessor Thread...");
        this.template = context.createProducerTemplate();


        this.config = config;

        // Create a ConnectionFactory
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        destination = session.createQueue("reuests.todo");

        // Create a MessageProducer from the Session to the Topic or Queue
        consumer = session.createConsumer(destination);
        flickr = new Flickr(config.API_KEY, config.SHARED_SECRET, new REST());
        getFlickrLicenseMap();


    }

    private void getFlickrLicenseMap() throws FlickrException {
        log.debug("Getting Flickr license list.");

        Collection<License> licenseInfo = flickr.getLicensesInterface().getInfo();
        licenseInfo.forEach(license -> {
            licenseMap.put(license.getId(), license.getName());
        });
        log.info("Got {} licenses from Flickr. ", licenseMap.size());
    }

    private boolean process(FlickrRequest request) throws FlickrException, JMSException {
        boolean stop = false;
        switch (request.requestType) {
            case POOL_SEARCH:
                 processPoolSearch(request);
                break;
            case PHOTO_INFO:
                processPhotoInfo(request);
                break;
            case END_OF_OPERATION:
                stop = true;
                break;
            default:
                stop = true;
                break;

        }
        return stop;
    }

    private void processPhotoInfo(FlickrRequest request) throws FlickrException {

    }

    private void processPoolSearch(FlickrRequest request) throws FlickrException, JMSException {
        log.debug("Issuing a PoolSearch request on Flickr...");
        if(request.page == 1){ // Send the header of the CSV first
            Map<String, String> dwcFieldsMap = new LinkedHashMap<>();
            config.CSV_FIELD_LIST.forEach(e -> dwcFieldsMap.put(e,e));
            template.sendBody("amq:output.csv", dwcFieldsMap);

        }
        PhotoList photoList = flickr.getPoolsInterface().getPhotos(request.groupId, request.userId, request.tags, new HashSet<String>(Arrays.asList(request.extras)), request
                .perPage, request
                .page);

        log.info("Got {} photos in page number {}.", photoList.size(), request.page);

        for (Object photo : photoList) {
            Photo photoInfo = (Photo) photo;
            boolean minUpdateMet = (config.MIN_UPDATE_DATE == null)? true :  photoInfo.getLastUpdate().after(config.MIN_UPDATE_DATE);
            boolean maxUpdateMet = (config.MAX_UPDATE_DATE == null)? true :  photoInfo.getLastUpdate().before(config.MAX_TAKEN_DATE);
            if(minUpdateMet && maxUpdateMet){
                log.debug("Photo is being processed...", photoInfo.getUrl());
                Map<String, String> dwcMap = new HashMap();
                dwcMap.put("basisOfRecord", "Image");
                final String license = licenseMap.get(photoInfo.getLicense());
                dwcMap.put("dcterms:accessRights", license);

                String photoUrl = photoInfo.getUrl().replaceAll("://f", "://www.f");
                photoUrl += (!photoUrl.endsWith("/"))?"/":null;
                dwcMap.put("occurrenceID", photoUrl);

                dwcMap.put("userId", photoInfo.getOwner().getId());
                dwcMap.put("associatedMedia", photoInfo.getOriginalUrl());
                if (photoInfo.getDateTaken() != null) {
                    dwcMap.put("eventDate", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(photoInfo.getDateTaken()));
                }
                dwcMap.put("occurrenceRemarks", photoInfo.getDescription());

                if (photoInfo.getDescription() != null){
                    Pattern bhlPattern = Pattern.compile("http://www.biodiversitylibrary.org/page/\\d+");
                    Matcher matcher = bhlPattern.matcher(photoInfo.getDescription());
                    dwcMap.put("occurrenceDetails", (matcher.find())?matcher.group():null);
                }
                if (dwcMap.get("occurrenceDetails") == null)
                    dwcMap.put("occurrenceDetails", photoUrl);

                dwcMap.put("recordedBy", (photoInfo.getOwner().getRealName() != null)? photoInfo.getOwner().getRealName() : photoInfo.getOwner().getUsername());
                dwcMap.put("license", license);

                if (photoInfo.getGeoData() != null){
                    dwcMap.put("decimalLatitude", photoInfo.getGeoData().getLatitude()+"");
                    dwcMap.put("decimalLongitude", photoInfo.getGeoData().getLongitude()+"");
                    dwcMap.put("coordinateUncertaintyInMeters", photoInfo.getGeoData().getAccuracy()+"");
                }

                dwcMap.put("country", (photoInfo.getCountry() != null )? photoInfo.getCountry().getName() : null);
                dwcMap.put("locality", (photoInfo.getLocality() != null )? photoInfo.getLocality().getName() : null);
                dwcMap.put("stateProvince", (photoInfo.getRegion() != null )? photoInfo.getRegion().getName() : null);

                if(photoInfo.getLocality() == null && photoInfo.getOwner().getLocation() != null)
                    dwcMap.put("locality", photoInfo.getOwner().getLocation());

                //Reading tag info
                photoInfo.getTags().forEach(tag -> {
                    Pattern machineTagPattern = Pattern.compile("(.+):(.+)=(.+)");
                    Matcher matcher = machineTagPattern.matcher(tag.getValue());
                    if(matcher.find()){

                        if (Stream.of(config.ALLOWED_TAGS).anyMatch(x ->  x.equalsIgnoreCase(matcher.group(2)))) {
                            dwcMap.put(matcher.group(2), matcher.group(3));
                        }
                    }
                });
                template.sendBody("amq:output.csv", dwcMap);
                log.debug("Photo {} is added to the CSV", photoInfo.getUrl());
            } else{
                log.info("Photo {} didn't meet the criteria to be included in the csv file.", photoInfo.getUrl());
            }

        }
    }

    @Override
    public void run() {
        boolean stop = false;
        long milliseconds = 10000;
        log.info("The thread started");
        while (!stop) {
            try {
                ObjectMessage objectMessage = (ObjectMessage) consumer.receive(milliseconds);
                FlickrRequest request = (FlickrRequest) objectMessage.getObject();
                if (request == null)
                    stop = true;
                stop = process(request);

            } catch (JMSException e) {
                e.printStackTrace();
            } catch (FlickrException e) {
                e.printStackTrace();
            } catch (NullPointerException e){
                log.warn("Couldn't receive any messages within {} milliseconds.", milliseconds);
            }
        }
        log.info("Reached the last element, the thread is going to stop.");
        try {
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}