package egm.io.nifi.processors.ngsild.model;

import java.util.List;

public class Attribute {
    public String attrName;
    public String attrType;
    public Object attrValue;
    public String datasetId;
    public String observedAt;
    public String createdAt;
    public String modifiedAt;
    public boolean hasSubAttrs;
    public List<Attribute> subAttrs;

    public Attribute(
        String attrName,
        String attrType,
        String datasetId,
        String observedAt,
        String createdAt,
        String modifiedAt,
        Object attrValue,
        boolean hasSubAttrs,
        List<Attribute> subAttrs
    ) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.datasetId = datasetId;
        this.observedAt = observedAt;
        this.createdAt = createdAt;
        this.modifiedAt = modifiedAt;
        this.attrValue = attrValue;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs = subAttrs;
    }

    public String getObservedAt() {
        return observedAt;
    }

    public boolean isHasSubAttrs() {
        return hasSubAttrs;
    }

    public List<Attribute> getSubAttrs() {
        return subAttrs;
    }

    public String getAttrName() {
        return attrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public Object getAttrValue() {
        return attrValue;
    }

    public String getDatasetId() {
        return datasetId;
    }
}
