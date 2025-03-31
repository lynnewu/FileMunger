
/// <summary>
/// Result of a file processing operation.
/// </summary>
public class FileProcessingResult {
	public String? FilePath { get; set; } = String.Empty; // Initialize with empty string
	public Boolean Success { get; set; }
	public String? ErrorMessage { get; set; } // Mark as nullable
}

