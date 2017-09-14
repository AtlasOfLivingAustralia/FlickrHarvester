package au.org.ala.flickrharvester;

import java.io.Serializable;

public class FlickrRequest implements Serializable {
    public final String groupId;
    public final String userId;
    public final String[] tags;
    public final String[] extras;
    public final int perPage;
    public final int page;
    public final String photoId;
    public final String secret;
    public final RequestType requestType;

    public enum RequestType{
        POOL_SEARCH,
        PHOTO_INFO,
        END_OF_OPERATION
    }

    public FlickrRequest(String groupId, String userId, String[] tags, String[] extras, int perPage, int page, String photoId, String secret, RequestType requestType) {
        this.groupId = groupId;
        this.userId = userId;
        this.tags = tags;
        this.extras = extras;
        this.perPage = perPage;
        this.page = page;
        this.photoId = photoId;
        this.secret = secret;
        this.requestType = requestType;
    }

}
