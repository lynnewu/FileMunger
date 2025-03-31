/// <summary>
/// Command line arguments configuration
/// </summary>
public class CommandLineOptions {
	/// <summary>
	/// Directories to process (comma-separated list, 'all' for all local drives, or defaults to current drive root)
	/// </summary>
	public List<String> Directories { get; set; } = new List<String>();

	/// <summary>
	/// Whether to recursively process subdirectories
	/// </summary>
	public Boolean Recursive { get; set; } = true;

	/// <summary>
	/// File specification pattern (supports Windows wildcards)
	/// </summary>
	public String FileSpec { get; set; } = "*.*";

	/// <summary>
	/// Verbosity level for output
	/// </summary>
	public LogLevel Verbosity { get; set; } = LogLevel.Info;

	/// <summary>
	/// Whether the options are valid
	/// </summary>
	public Boolean IsValid { get; set; } = true;

	/// <summary>
	/// Error message if options are invalid
	/// </summary>
	public String? ErrorMessage { get; set; } = null;




}
