
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Serializable{

    @JsonProperty("full_text")
    public String fulltext;
    public String lang;
    public User user;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public class User implements Serializable{
            public long id;
            public String name;
            @JsonProperty("screen_name")
            public String screenName;
    }
}
