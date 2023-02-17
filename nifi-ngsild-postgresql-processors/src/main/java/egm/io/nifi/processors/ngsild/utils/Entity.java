package egm.io.nifi.processors.ngsild.utils;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<Attribute> entityAttrs;

    public Entity(String entityId, String entityType, ArrayList<Attribute> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrs = entityAttrs;
    }

    public ArrayList<Attribute> getEntityAttrs() {
        return entityAttrs;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }
}
