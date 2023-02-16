package egm.io.nifi.processors.ngsild.utils;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<Attributes> entityAttrsLD;

    public Entity(String entityId, String entityType, ArrayList<Attributes> entityAttrsLD) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrsLD = entityAttrsLD;
    }

    public ArrayList<Attributes> getEntityAttrsLD() {
        return entityAttrsLD;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }
}
