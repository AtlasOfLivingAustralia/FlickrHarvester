package au.org.ala.flickrharvester;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by Mahmoud Sadeghi on 5/9/17.
 */
public final class Config {

    public static final List<String> CSV_FIELD_LIST = Arrays.asList("basisOfRecord","dcterms:accessRights","occurrenceID","userId","associatedMedia","eventDate","occurrenceRemarks",
            "occurrenceDetails","recordedBy","license","taxonID", "scientificNameID", "acceptedNameUsageID", "parentNameUsageID", "originalNameUsageID", "nameAccordingToID",
            "namePublishedInID", "taxonConceptID", "scientificName", "acceptedNameUsage", "parentNameUsage", "originalNameUsage", "nameAccordingTo",
            "namePublishedIn", "namePublishedInYear", "higherClassification", "kingdom", "phylum", "class", "order", "family", "genus", "subgenus",
            "specificEpithet", "infraspecificEpithet", "taxonRank", "verbatimTaxonRank", "scientificNameAuthorship", "vernacularName", "nomenclaturalCode",
            "taxonomicStatus", "nomenclaturalStatus", "taxonRemarks", "locationID", "higherGeographyID", "higherGeography", "continent", "waterBody", "islandGroup",
            "island", "country", "countryCode", "stateProvince", "county", "municipality", "locality", "verbatimLocality", "minimumElevationInMeters",
            "maximumElevationInMeters", "verbatimElevation", "minimumDepthInMeters", "maximumDepthInMeters", "verbatimDepth", "minimumDistanceAboveSurfaceInMeters",
            "maximumDistanceAboveSurfaceInMeters", "locationAccordingTo", "locationRemarks", "decimalLatitude", "decimalLongitude", "geodeticDatum",
            "coordinateUncertaintyInMeters", "coordinatePrecision", "pointRadiusSpatialFit", "verbatimCoordinates", "verbatimLatitude", "verbatimLongitude",
            "verbatimCoordinateSystem", "verbatimSRS", "footprintWKT", "footprintSRS", "footprintSpatialFit", "georeferencedBy", "georeferencedDate",
            "georeferenceProtocol", "georeferenceSources", "georeferenceVerificationStatus", "georeferenceRemarks");
    public static final String[] ALLOWED_TAGS = {"taxonID", "scientificNameID", "acceptedNameUsageID", "parentNameUsageID", "originalNameUsageID", "nameAccordingToID",
            "namePublishedInID", "taxonConceptID", "scientificName", "acceptedNameUsage", "parentNameUsage", "originalNameUsage", "nameAccordingTo",
            "namePublishedIn", "namePublishedInYear", "higherClassification", "kingdom", "phylum", "class", "order", "family", "genus", "subgenus",
            "specificEpithet", "infraspecificEpithet", "taxonRank", "verbatimTaxonRank", "scientificNameAuthorship", "vernacularName", "nomenclaturalCode",
            "taxonomicStatus", "nomenclaturalStatus", "taxonRemarks","locationID","higherGeographyID","higherGeography","continent","waterBody","islandGroup",
            "island","country","countryCode","stateProvince","county","municipality","locality","verbatimLocality","minimumElevationInMeters",
            "maximumElevationInMeters","verbatimElevation","minimumDepthInMeters","maximumDepthInMeters","verbatimDepth","minimumDistanceAboveSurfaceInMeters",
            "maximumDistanceAboveSurfaceInMeters","locationAccordingTo","locationRemarks","decimalLatitude","decimalLongitude","geodeticDatum",
            "coordinateUncertaintyInMeters","coordinatePrecision","pointRadiusSpatialFit","verbatimCoordinates","verbatimLatitude","verbatimLongitude",
            "verbatimCoordinateSystem","verbatimSRS","footprintWKT","footprintSRS","footprintSpatialFit","georeferencedBy","georeferencedDate",
            "georeferenceProtocol","georeferenceSources","georeferenceVerificationStatus","georeferenceRemarks"};
    public final String USER_ID;
    public final Integer PER_PAGE;
    public final String[] MACHINE_TAGS;
    public final String API_KEY;
    public final String GROUP_ID;
    public final String[] EXTRAS;
    public final String PRIVACY_FILTER;
    public final String CONTENT_TYPE;
    public final String[] UNIQUE_KEYS;
    public final String FLICKR_BASE_URL;
    public final Date MIN_UPLOAD_DATE;
    public final Date MAX_UPLOAD_DATE;
    public final Date MIN_TAKEN_DATE;
    public final Date MAX_TAKEN_DATE;
    public final Date MIN_UPDATE_DATE;
    public final Date MAX_UPDATE_DATE;
    public final String DEFAULT_QUERY_STRING;
    public final String SHARED_SECRET;

    public Config(String filePath) throws ConfigurationException, ParseException {
        Configurations configs = new Configurations();
        PropertiesConfiguration config = configs.properties(new File(filePath));

        API_KEY = config.getString("flickr.api_key");
        SHARED_SECRET = config.getString("flickr.secret");
        CONTENT_TYPE = config.getString("flickr.content_type");
        FLICKR_BASE_URL = config.getString("flickr.baseUrl");
        MACHINE_TAGS = config.getStringArray("flickr.machine_tags");
        GROUP_ID = config.getString("flickr.group_id");

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String max_taken_date = config.getString("flickr.max_taken_date");
        MAX_TAKEN_DATE = (max_taken_date.equals("")) ? null : df.parse(max_taken_date);
        String max_update_date = config.getString("flickr.max_update_date");
        MAX_UPDATE_DATE = (max_update_date.equals("")) ? null : df.parse(max_update_date);
        String max_upload_date = config.getString("flickr.max_upload_date");
        MAX_UPLOAD_DATE = (max_upload_date.equals("")) ? null : df.parse(max_upload_date);
        String min_taken_date = config.getString("flickr.min_taken_date");
        MIN_TAKEN_DATE = (min_taken_date.equals("")) ? null : df.parse(min_taken_date);
        String min_update_date = config.getString("flickr.min_update_date");
        MIN_UPDATE_DATE = (min_update_date.equals("")) ? null : df.parse(min_update_date);
        String min_upload_date = config.getString("flickr.min_upload_date");
        MIN_UPLOAD_DATE = (min_upload_date.equals("")) ? null : df.parse(min_upload_date);
        PER_PAGE = config.getInteger("flickr.per_page", 100);
        PRIVACY_FILTER = config.getString("flickr.privacy_filter");
        UNIQUE_KEYS = config.getStringArray("collectory.termsForUniqueKey");
        USER_ID = config.getString("flickr.user_id");
        EXTRAS = config.getStringArray("flickr.extras");


        String defaultQueryString = "";
        defaultQueryString += (API_KEY != null) ? ("api_key=" + API_KEY) : "";
        defaultQueryString += (CONTENT_TYPE != null) ? ("content_type=" + CONTENT_TYPE) : "";
        defaultQueryString += (MACHINE_TAGS != null) ? ("machine_tags=" + MACHINE_TAGS) : "";
        defaultQueryString += (GROUP_ID != null) ? ("group_id=" + GROUP_ID) : "";
        defaultQueryString += (PER_PAGE != null) ? ("per_page=" + PER_PAGE) : "";
        defaultQueryString += (PRIVACY_FILTER != null) ? ("privacy_filter=" + PRIVACY_FILTER) : "";
        defaultQueryString += (USER_ID != null) ? ("user_id=" + USER_ID) : "";

        DEFAULT_QUERY_STRING = defaultQueryString;

    }


}
