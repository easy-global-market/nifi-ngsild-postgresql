package egm.io.nifi.processors.ngsild.utils;

import java.util.ArrayList;

public class NGSIEvent {
    public long creationTime;
    public String ngsiLdTenant;
    public ArrayList <Entity> entitiesLD;

    public NGSIEvent(long creationTime, String ngsiLdTenant, ArrayList<Entity> entitiesLD){
        this.creationTime = creationTime;
        this.ngsiLdTenant = ngsiLdTenant;
        this.entitiesLD = entitiesLD;
    }

    public ArrayList<Entity> getEntitiesLD() {
        return entitiesLD;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getNgsiLdTenant() {
        return ngsiLdTenant;
    }
}
