package egm.io.nifi.processors.ngsild.utils;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public String ngsiLdTenant;
    public ArrayList <Entity> entities;

    public NGSIEvent(long creationTime, String ngsiLdTenant, ArrayList<Entity> entities){
        this.creationTime = creationTime;
        this.ngsiLdTenant = ngsiLdTenant;
        this.entities = entities;
    }

    public ArrayList<Entity> getEntities() {
        return entities;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getNgsiLdTenant() {
        return ngsiLdTenant;
    }
}
