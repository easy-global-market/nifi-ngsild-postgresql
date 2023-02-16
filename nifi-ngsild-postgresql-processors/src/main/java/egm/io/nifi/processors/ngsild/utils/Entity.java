package egm.io.nifi.processors.ngsild.utils;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<AttributesLD> entityAttrsLD;

    public Entity(String entityId, String entityType, ArrayList<AttributesLD> entityAttrsLD) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrsLD = entityAttrsLD;
    }

    public ArrayList<AttributesLD> getEntityAttrsLD() {
        return entityAttrsLD;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }
}
