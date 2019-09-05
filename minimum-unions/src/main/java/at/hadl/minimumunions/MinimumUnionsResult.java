package at.hadl.minimumunions;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class MinimumUnionsResult {
	List<String> optimalPartitions;
	Integer weightSum;
}
