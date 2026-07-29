package egm.io.nifi.processors.ngsild.model;

import org.apache.nifi.components.DescribedValue;

public enum ExportMode implements DescribedValue {
    EXPANDED("Expanded: one column per attribute"),
    FLATTEN("Flatten: generic columns for all observations"),
    SEMI_FLATTEN("Flatten On Multi-Instances Attributes: one column per attribute name with an associated datasetId column");

    private final String description;

    ExportMode(final String description) {
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return description;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
