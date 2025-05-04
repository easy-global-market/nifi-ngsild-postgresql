package egm.io.nifi.processors.ngsild.model;

import java.util.List;

public class Event {
    public long creationTime;
    public List<Entity> entities;

    public Event(long creationTime, List<Entity> entities) {
        this.creationTime = creationTime;
        this.entities = entities;
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public long getCreationTime() {
        return creationTime;
    }
}
