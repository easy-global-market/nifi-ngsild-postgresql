package egm.io.nifi.processors.ngsild.utils;

import java.util.List;

public class NGSIEvent {
    public long creationTime;
    public String ngsiLdTenant;
    public List<Entity> entities;

    public NGSIEvent(long creationTime, String ngsiLdTenant, List<Entity> entities){
        this.creationTime = creationTime;
        this.ngsiLdTenant = ngsiLdTenant;
        this.entities = entities;
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getNgsiLdTenant() {
        return ngsiLdTenant;
    }
}
