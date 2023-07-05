package ca.sunlife.eadp.eventhub.mesh.kafka.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "instance_id",
        "base_uri"
})
public class ConsumerGroupModel implements Serializable {

    @JsonProperty("instance_id")
    @NotNull(message = "instance_id")
    private String instanceId;
    @JsonProperty("base_uri")
    @NotNull(message = "base_uri may not be null")
    private String baseUri;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public void setBaseUri(String baseUri) {
        this.baseUri = baseUri;
    }
}
