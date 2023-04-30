package egm.io.nifi.processors.ngsild.utils;

import java.util.List;

public class Entity {
    public String entityId;
    public String entityType;
    public List<Attribute> entityAttrs;

    public Entity(String entityId, String entityType, List<Attribute> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
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
}
