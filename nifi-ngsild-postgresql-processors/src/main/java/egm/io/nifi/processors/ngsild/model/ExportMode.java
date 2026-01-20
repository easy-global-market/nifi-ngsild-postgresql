package egm.io.nifi.processors.ngsild.model;

import org.apache.nifi.components.DescribedValue;

public enum ExportMode implements DescribedValue {
    EXPANDED("Expanded"),
    FLATTEN("Flatten On Observed Attributes "),
    SEMI_FLATTEN("Flatten On Multi-Instances Attributes");

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
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }
}
