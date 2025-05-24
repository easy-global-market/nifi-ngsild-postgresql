package egm.io.nifi.processors.ngsild.model;

import java.util.List;
import java.util.Set;

public class Entity {
    public String entityId;
    public String entityType;
    public Set<String> scopes;
    public List<Attribute> entityAttrs;

    public Entity(String entityId, String entityType, Set<String> scopes, List<Attribute> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.scopes = scopes;
        this.entityAttrs = entityAttrs;
    }

    public List<Attribute> getEntityAttrs() {
        return entityAttrs;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public Set<String> getScopes() {
        return scopes;
    }
}
