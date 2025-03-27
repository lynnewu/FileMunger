using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks.Dataflow;

[assembly: System.Runtime.Versioning.SupportedOSPlatform("windows")]

namespace FileMunger {
	/// <summary>
	/// FileMunger - Advanced file system traversal and processing utility
	/// 
	/// This application recursively processes all files in a directory structure with
	/// proper handling of symbolic links, I/O performance throttling, and parallel processing.
	/// 
	/// Features:
	/// - Safe handling of symbolic links and junctions to prevent infinite recursion
	/// - Dynamic I/O performance throttling based on system load
	/// - Parallel processing scaled to available processor cores
	/// - Extensive logging and error handling
	/// - Windows-optimized file access
	/// </summary>
	static public class Program {
		/// <summary>
		/// Entry point for the FileMunger application.
		/// </summary>
		/// <param name="args">Command line arguments. First argument should be the root directory path to process.</param>
		//static async Task Main(String[] args) {
		//	// Initialize logging
		//	Logger.Initialize(LogLevel.Debug);
		//	Logger.LogInfo("FileMunger starting up...");
		//	Logger.LogDebug($"Running on .NET version: {Environment.Version}");

		//	// Validate command line arguments
		//	if (args.Length == 0) {
		//		Logger.LogError("No directory path provided. Usage: FileMunger.exe [directory_path]");
		//		Console.WriteLine("Please provide a directory path to scan");
		//		return;
		//	}

		//	String rootDirectory = args[0];

		//	// Verify the directory exists
		//	if (!Directory.Exists(rootDirectory)) {
		//		Logger.LogError($"Directory not found: {rootDirectory}");
		//		Console.WriteLine($"Directory not found: {rootDirectory}");
		//		return;
		//	}

		//	Logger.LogInfo($"Starting processing of directory: {rootDirectory}");

		//	// Create performance tracker
		//	PerformanceTracker performanceTracker = new PerformanceTracker();
		//	performanceTracker.Start();

		//	// Create and configure I/O throttler
		//	IoPerformanceThrottler ioThrottler = new IoPerformanceThrottler();

		//	try {
		//		// Process the directory hierarchy
		//		await ProcessDirectoryAsync(rootDirectory, ioThrottler);

		//		// Log completion statistics
		//		performanceTracker.Stop();
		//		Logger.LogInfo($"Processing complete in {performanceTracker.ElapsedTime.TotalSeconds:F2} seconds");
		//		Logger.LogInfo($"Peak memory usage: {performanceTracker.PeakMemoryUsageMB:F2} MB");
		//	}
		//	catch (Exception ex) {
		//		Logger.LogError($"Unhandled exception in main processing: {ex}");
		//		Console.WriteLine("An error occurred during processing. Check logs for details.");
		//	}

		//	Console.WriteLine("Processing complete!");
		//}

		/// <summary>
		/// Entry point for the FileMunger application.
		/// </summary>
		/// <param name="args">Command line arguments. First argument contains comma-separated directory paths to process.</param>
		static async Task Main(String[] args) {
			// Initialize logging
			Logger.Initialize(LogLevel.Debug);
			Logger.LogInfo("FileMunger starting up...");
			Logger.LogDebug($"Running on .NET version: {Environment.Version}");

			// Validate command line arguments
			if (args.Length == 0) {
				Logger.LogError("No directory paths provided. Usage: FileMunger.exe [directory_path1,directory_path2,...]");
				Console.WriteLine("Please provide at least one directory path to scan (separate multiple paths with commas)");
				return;
			}

			// Split the comma-separated paths and validate each one
			String[] directoryPaths = args[0].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

			if (directoryPaths.Length == 0) {
				Logger.LogError("No valid directory paths found after splitting");
				Console.WriteLine("Please provide valid directory paths separated by commas");
				return;
			}

			// Verify each directory exists and build a list of valid ones
			List<String> validDirectories = new List<String>();

			foreach (String directory in directoryPaths) {
				if (Directory.Exists(directory)) {
					validDirectories.Add(directory);
					Logger.LogInfo($"Validated directory: {directory}");
				}
				else {
					Logger.LogWarning($"Directory not found, will be skipped: {directory}");
					Console.WriteLine($"Warning: Directory not found, will be skipped: {directory}");
				}
			}

			if (validDirectories.Count == 0) {
				Logger.LogError("No valid directories to process");
				Console.WriteLine("No valid directories found to process");
				return;
			}

			// Group directories by drive letter to process separate drives in parallel
			var directoriesByDrive = validDirectories
					.GroupBy(path => {
						try {
							// Get the drive information for the path
							return Path.GetFullPath(path).Substring(0, 1).ToUpper();
						}
						catch {
							// If there's any issue, use a placeholder
							return "?";
						}
					})
					.ToDictionary(g => g.Key, g => g.ToList());

			Logger.LogInfo($"Found {validDirectories.Count} valid directories across {directoriesByDrive.Count} drives");

			foreach (var drive in directoriesByDrive) {
				Logger.LogInfo($"Drive {drive.Key}: {drive.Value.Count} directories to process");
			}

			// Create performance tracker
			PerformanceTracker performanceTracker = new PerformanceTracker();
			performanceTracker.Start();

			try {
				// Process each drive's directories in parallel
				List<Task> driveTasks = new List<Task>();

				foreach (var driveGroup in directoriesByDrive) {
					String driveLetter = driveGroup.Key;
					List<String> driveDirectories = driveGroup.Value;

					// Create a task to process all directories on this drive
					Task driveTask = Task.Run(async () => {
						Logger.LogInfo($"Starting processing for drive {driveLetter} with {driveDirectories.Count} directories");

						// Create an I/O throttler specific to this drive
						IoPerformanceThrottler driveThrottler = new IoPerformanceThrottler();

						// Process directories on this drive sequentially
						foreach (String directory in driveDirectories) {
							Logger.LogInfo($"Processing directory on drive {driveLetter}: {directory}");
							await ProcessDirectoryAsync(directory, driveThrottler);
						}

						Logger.LogInfo($"Completed processing for drive {driveLetter}");
					});

					driveTasks.Add(driveTask);
				}

				// Wait for all drive processing to complete
				await Task.WhenAll(driveTasks);

				// Log completion statistics
				performanceTracker.Stop();
				Logger.LogInfo($"Processing complete in {performanceTracker.ElapsedTime.TotalSeconds:F2} seconds");
				Logger.LogInfo($"Peak memory usage: {performanceTracker.PeakMemoryUsageMB:F2} MB");
			}
			catch (Exception ex) {
				Logger.LogError($"Unhandled exception in main processing: {ex}");
				Console.WriteLine("An error occurred during processing. Check logs for details.");
			}

			Console.WriteLine("Processing complete!");
		}

		/// <summary>
		/// Recursively processes all files in a directory and its subdirectories.
		/// Handles symbolic links safely and throttles processing based on I/O performance.
		/// </summary>
		/// <param name="rootDirectory">The root directory to process</param>
		/// <param name="ioThrottler">The I/O throttler to use for performance management</param>
		static async Task ProcessDirectoryAsync(String rootDirectory, IoPerformanceThrottler ioThrottler) {
			Logger.LogDebug($"Setting up directory traversal for: {rootDirectory}");

			// Configure enumeration options for traversing the directory structure
			EnumerationOptions options = new EnumerationOptions {
				RecurseSubdirectories = true,
				AttributesToSkip = FileAttributes.System,
				ReturnSpecialDirectories = false,
				MatchType = MatchType.Simple,
				IgnoreInaccessible = true, // Skip files/directories we don't have access to
				MatchCasing = MatchCasing.CaseInsensitive // Windows is case-insensitive
			};

			// Initialize processing statistics
			Int64 fileCount = 0;
			Int64 totalBytes = 0;
			Int64 errorCount = 0;

			// Create a block for file processing with throttling based on CPU count
			Int32 processorCount = Environment.ProcessorCount;
			Logger.LogInfo($"Using {processorCount} processors for parallel processing");

			// Configure the dataflow block for parallel file processing
			ExecutionDataflowBlockOptions blockOptions = new ExecutionDataflowBlockOptions {
				MaxDegreeOfParallelism = processorCount,
				BoundedCapacity = processorCount * 2, // Limit queue size to prevent memory issues
				EnsureOrdered = false // Files can be processed in any order for better performance
			};

			// Create an ActionBlock that will process files in parallel
			ActionBlock<String> fileProcessor = new ActionBlock<String>(
					async filePath => {
						try {
							// Process each file and track statistics
							FileInfo fileInfo = new FileInfo(filePath);
							FileProcessingResult result = await ProcessFileAsync(filePath, ioThrottler);

							if (result.Success) {
								Interlocked.Increment(ref fileCount);
								Interlocked.Add(ref totalBytes, fileInfo.Length);
							}
							else {
								Interlocked.Increment(ref errorCount);
							}
						}
						catch (Exception ex) {
							Logger.LogError($"Error in file processor: {ex.Message}");
							Interlocked.Increment(ref errorCount);
						}
					},
					blockOptions
			);

			// Track visited links to avoid cycles
			ConcurrentDictionary<String, Boolean> visitedLinks = new ConcurrentDictionary<String, Boolean>();
			Int32 directoryCount = 0;

			// Start enumerating files with safe link tracking
			try {
				Logger.LogInfo("Starting file enumeration...");
				Int64 enumerationStart = Stopwatch.GetTimestamp();

				// Create a queue for breadth-first directory traversal
				Queue<String> directoriesToProcess = new Queue<String>();
				directoriesToProcess.Enqueue(rootDirectory);

				while (directoriesToProcess.Count > 0) {
					String currentDir = directoriesToProcess.Dequeue();
					directoryCount++;

					// Log progress periodically
					if (directoryCount % 100 == 0) {
						Logger.LogInfo($"Processed {directoryCount} directories, {fileCount} files ({totalBytes / (1024 * 1024)} MB)");
					}

					try {
						// Process all files in the current directory
						foreach (String filePath in Directory.EnumerateFiles(currentDir)) {
							FileInfo fileInfo = new FileInfo(filePath);

							// Skip symbolic links we've already seen to prevent cycles
							if ((fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
								String? targetPath = ResolveSymbolicLink(filePath);

								// If we've already visited this link target, skip it
								if (!String.IsNullOrEmpty(targetPath)) {
									if (!visitedLinks.TryAdd(targetPath.ToLowerInvariant(), true)) {
										Logger.LogDebug($"Skipping previously visited symbolic link: {filePath} -> {targetPath}");
										continue;
									}
									else {
										Logger.LogDebug($"Following symbolic link: {filePath} -> {targetPath}");
									}
								}
							}

							// Queue the file for processing
							Boolean accepted = await fileProcessor.SendAsync(filePath);
							if (!accepted) {
								Logger.LogWarning($"File processor queue full, waiting: {filePath}");
								// If the queue is full, wait and retry
								while (!await fileProcessor.SendAsync(filePath)) {
									await Task.Delay(100);
								}
							}
						}

						// Enumerate subdirectories with symbolic link handling
						foreach (String dirPath in Directory.EnumerateDirectories(currentDir)) {
							DirectoryInfo dirInfo = new DirectoryInfo(dirPath);

							if ((dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
								// Handle directory symbolic links
								String? targetPath = ResolveSymbolicLink(dirPath);

								// Check if we've already visited this directory target
								if (!String.IsNullOrEmpty(targetPath)) {
									if (!visitedLinks.TryAdd(targetPath.ToLowerInvariant(), true)) {
										Logger.LogDebug($"Skipping previously visited directory link: {dirPath} -> {targetPath}");
										continue;
									}
									else {
										Logger.LogDebug($"Following directory link: {dirPath} -> {targetPath}");
										// Use the target path instead
										directoriesToProcess.Enqueue(targetPath);
										continue;
									}
								}
							}

							// Queue regular directories for processing
							directoriesToProcess.Enqueue(dirPath);
						}
					}
					catch (UnauthorizedAccessException ex) {
						Logger.LogWarning($"Access denied to directory: {currentDir} - {ex.Message}");
					}
					catch (Exception ex) {
						Logger.LogError($"Error processing directory {currentDir}: {ex.Message}");
					}
				}

				Double enumerationTime = Stopwatch.GetElapsedTime(enumerationStart).TotalSeconds;
				Logger.LogInfo($"File enumeration completed in {enumerationTime:F2} seconds");
			}
			catch (Exception ex) {
				Logger.LogError($"Error during directory traversal: {ex}");
				throw;
			}

			// Complete the processor and wait for all queued files to complete
			Logger.LogInfo("Directory traversal complete, waiting for file processing to finish...");
			fileProcessor.Complete();
			await fileProcessor.Completion;

			// Log final statistics
			Logger.LogInfo($"Directory traversal statistics:");
			Logger.LogInfo($"  - Directories processed: {directoryCount}");
			Logger.LogInfo($"  - Files processed: {fileCount}");
			Logger.LogInfo($"  - Total data: {totalBytes / (1024.0 * 1024.0):F2} MB");
			Logger.LogInfo($"  - Errors encountered: {errorCount}");
		}

		/// <summary>
		/// Processes a single file with I/O throttling.
		/// </summary>
		/// <param name="filePath">Path to the file to process</param>
		/// <param name="throttler">The I/O throttler to use</param>
		/// <returns>A FileProcessingResult indicating success or failure</returns>
		static async Task<FileProcessingResult> ProcessFileAsync(String filePath, IoPerformanceThrottler throttler) {
			FileProcessingResult result = new FileProcessingResult { FilePath = filePath };

			try {
				// Get throttled access based on I/O performance
				using (await throttler.GetThrottledAccessAsync()) {
					// Log file processing
					Logger.LogDebug($"Processing: {filePath}");

					// Get basic file info
					FileInfo fileInfo = new FileInfo(filePath);

					// Skip zero-byte files
					if (fileInfo.Length == 0) {
						Logger.LogDebug($"Skipping zero-byte file: {filePath}");
						result.Success = true;
						return result;
					}

					// Example file processing - replace with actual processing logic
					using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true)) {
						// Simulate file processing - compute checksum, scan content, etc.
						Byte[] buffer = new Byte[Math.Min(4096, fileInfo.Length)];
						Int32 bytesRead = await fs.ReadAsync(buffer, 0, buffer.Length);

						// In a real implementation, you would process the file contents here

						Logger.LogDebug($"{filePath}");



						//  example
						//// Calculate a simple checksum as an example
						//UInt32 checksum = 0;
						//for (Int32 i = 0; i < bytesRead; i++) {
						//	checksum += buffer[i];
						//}
						//						Logger.LogDebug($"File processed: {filePath} (Size: {fileInfo.Length} bytes, Initial checksum: {checksum})");
					}

					result.Success = true;
				}
			}
			catch (FileNotFoundException) {
				Logger.LogWarning($"File not found (may have been deleted): {filePath}");
				result.Success = false;
				result.ErrorMessage = "File not found";
			}
			catch (UnauthorizedAccessException) {
				Logger.LogWarning($"Access denied to file: {filePath}");
				result.Success = false;
				result.ErrorMessage = "Access denied";
			}
			catch (IOException ex) {
				Logger.LogWarning($"I/O error for file {filePath}: {ex.Message}");
				result.Success = false;
				result.ErrorMessage = $"I/O error: {ex.Message}";
			}
			catch (Exception ex) {
				Logger.LogError($"Unexpected error processing file {filePath}: {ex.Message}");
				result.Success = false;
				result.ErrorMessage = $"Error: {ex.Message}";
			}

			return result;
		}

		/// <summary>
		/// Resolves a symbolic link to its target path using Windows APIs.
		/// </summary>
		/// <param name="linkPath">The path to the symbolic link</param>
		/// <returns>The resolved target path, or null if resolution fails</returns>
		static String? ResolveSymbolicLink(String linkPath) {
			try {
				Logger.LogDebug($"Resolving symbolic link: {linkPath}");
				String targetPath = NativeMethods.GetFinalPathName(linkPath);
				Logger.LogDebug($"Link target: {targetPath}");
				return targetPath;
			}
			catch (Exception ex) {
				Logger.LogWarning($"Failed to resolve symbolic link {linkPath}: {ex.Message}");
				return null;
			}
		}
	}

	/// <summary>
	/// Result of a file processing operation.
	/// </summary>
	public class FileProcessingResult {
		public String FilePath { get; set; } = String.Empty; // Initialize with empty string
		public Boolean Success { get; set; }
		public String? ErrorMessage { get; set; } // Mark as nullable
	}


	/// <summary>
	/// Level of detail for logging
	/// </summary>
	public enum LogLevel {
		Debug,
		Info,
		Warning,
		Error
	}

	/// <summary>
	/// Central logging class that writes to both Debug output and a log file
	/// </summary>
	public static class Logger {
		private static LogLevel _currentLogLevel = LogLevel.Info;
		private static readonly Object _lockObj = new Object();
		private static String? _logFilePath;

		/// <summary>
		/// Initializes the logger with the specified minimum log level
		/// </summary>
		/// <param name="level">Minimum level of messages to log</param>
		/// <param name="logFilePath">Optional path to a log file (if null, file logging is disabled)</param>
		public static void Initialize(LogLevel level, String? logFilePath = null) {
			_currentLogLevel = level;
			_logFilePath = logFilePath;

			if (!String.IsNullOrEmpty(_logFilePath)) {
				// Create log directory if needed
				String? logDir = Path.GetDirectoryName(_logFilePath);
				if (!String.IsNullOrEmpty(logDir) && !Directory.Exists(logDir)) {
					Directory.CreateDirectory(logDir);
				}

				// Write header to log file
				File.WriteAllText(_logFilePath, $"FileMunger Log - Started {DateTime.Now}\r\n");
				LogInfo("Logging initialized");
			}
		}

		/// <summary>
		/// Logs a debug message
		/// </summary>
		/// <param name="message">The message to log</param>
		public static void LogDebug(String message) {
			if (_currentLogLevel <= LogLevel.Debug) {
				WriteLog("DEBUG", message);
			}
		}

		/// <summary>
		/// Logs an informational message
		/// </summary>
		/// <param name="message">The message to log</param>
		public static void LogInfo(String message) {
			if (_currentLogLevel <= LogLevel.Info) {
				WriteLog("INFO", message);
			}
		}

		/// <summary>
		/// Logs a warning message
		/// </summary>
		/// <param name="message">The message to log</param>
		public static void LogWarning(String message) {
			if (_currentLogLevel <= LogLevel.Warning) {
				WriteLog("WARN", message);
			}
		}

		/// <summary>
		/// Logs an error message
		/// </summary>
		/// <param name="message">The message to log</param>
		public static void LogError(String message) {
			if (_currentLogLevel <= LogLevel.Error) {
				WriteLog("ERROR", message);
			}
		}

		/// <summary>
		/// Writes a log entry with the specified level and message
		/// </summary>
		/// <param name="level">The log level as a string</param>
		/// <param name="message">The message to log</param>
		private static void WriteLog(String level, String message) {
			String threadId = Thread.CurrentThread.ManagedThreadId.ToString();
			String timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
			String formattedMessage = $"[{timestamp}] [{threadId}] {level}: {message}";

			// Write to debug window
			Debug.WriteLine(formattedMessage);

			// Write to log file if enabled
			if (!String.IsNullOrEmpty(_logFilePath)) {
				lock (_lockObj) {
					try {
						File.AppendAllText(_logFilePath, formattedMessage + "\r\n");
					}
					catch (Exception ex) {
						// Don't let logging errors disrupt processing
						Debug.WriteLine($"Error writing to log file: {ex.Message}");
					}
				}
			}
		}
	}

	/// <summary>
	/// Native Windows API methods for handling symbolic links
	/// </summary>
	internal static class NativeMethods {
		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
		private static extern Int32 GetFinalPathNameByHandle(IntPtr handle, [MarshalAs(UnmanagedType.LPWStr)] StringBuilder path, Int32 pathLength, Int32 flags);

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
		private static extern IntPtr CreateFile(
				String lpFileName,
				UInt32 dwDesiredAccess,
				UInt32 dwShareMode,
				IntPtr lpSecurityAttributes,
				UInt32 dwCreationDisposition,
				UInt32 dwFlagsAndAttributes,
				IntPtr hTemplateFile);

		[DllImport("kernel32.dll", SetLastError = true)]
		[return: MarshalAs(UnmanagedType.Bool)]
		private static extern Boolean CloseHandle(IntPtr hObject);

		// Windows-specific constants
		private const UInt32 FILE_READ_ATTRIBUTES = 0x80;
		private const UInt32 FILE_SHARE_READ = 0x1;
		private const UInt32 FILE_SHARE_WRITE = 0x2;
		private const UInt32 OPEN_EXISTING = 3;
		private const UInt32 FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
		private const Int32 FILE_NAME_NORMALIZED = 0x0;

		/// <summary>
		/// Gets the final path name for a file, resolving any symbolic links
		/// </summary>
		/// <param name="path">The path to resolve</param>
		/// <returns>The resolved final path</returns>
		public static String GetFinalPathName(String path) {
			Logger.LogDebug($"Resolving path with Windows API: {path}");

			// Open a handle to the file or directory
			IntPtr handle = CreateFile(path,
					FILE_READ_ATTRIBUTES,
					FILE_SHARE_READ | FILE_SHARE_WRITE,
					IntPtr.Zero,
					OPEN_EXISTING,
					FILE_FLAG_BACKUP_SEMANTICS, // Required for directories
					IntPtr.Zero);

			if (handle == new IntPtr(-1)) {
				Int32 error = Marshal.GetLastWin32Error();
				Logger.LogWarning($"CreateFile failed for '{path}'. Error: {error}");
				throw new IOException($"Could not open file: {path}. Error: {error}");
			}

			try {
				StringBuilder result = new StringBuilder(512);
				Int32 length = GetFinalPathNameByHandle(handle, result, result.Capacity, FILE_NAME_NORMALIZED);

				if (length >= result.Capacity) {
					// Need a larger buffer
					result.EnsureCapacity(length + 1);
					length = GetFinalPathNameByHandle(handle, result, result.Capacity, FILE_NAME_NORMALIZED);
				}

				if (length > 0) {
					String finalPath = result.ToString();
					// Remove \\?\ prefix if present
					if (finalPath.StartsWith("\\\\?\\")) {
						finalPath = finalPath.Substring(4);
					}
					return finalPath;
				}

				Int32 finalError = Marshal.GetLastWin32Error();
				Logger.LogWarning($"GetFinalPathNameByHandle failed. Error: {finalError}");
				throw new IOException($"Failed to get final path. Error: {finalError}");
			}
			finally {
				CloseHandle(handle);
			}
		}
	}

	/// <summary>
	/// Tracks application performance metrics like memory usage and execution time
	/// </summary>
	public class PerformanceTracker {
		private readonly Stopwatch _stopwatch = new Stopwatch();
		private readonly Process _currentProcess = Process.GetCurrentProcess();

		/// <summary>
		/// Time elapsed since tracking started
		/// </summary>
		public TimeSpan ElapsedTime => _stopwatch.Elapsed;

		/// <summary>
		/// Peak memory usage during tracking, in megabytes
		/// </summary>
		public Double PeakMemoryUsageMB => _currentProcess.PeakWorkingSet64 / (1024.0 * 1024.0);

		/// <summary>
		/// Current memory usage, in megabytes
		/// </summary>
		public Double CurrentMemoryUsageMB => _currentProcess.WorkingSet64 / (1024.0 * 1024.0);

		/// <summary>
		/// Starts performance tracking
		/// </summary>
		public void Start() {
			_stopwatch.Start();
			Logger.LogDebug("Performance tracking started");
		}

		/// <summary>
		/// Stops performance tracking
		/// </summary>
		public void Stop() {
			_stopwatch.Stop();
			Logger.LogInfo($"Performance tracking stopped. Elapsed: {ElapsedTime.TotalSeconds:F2}s, Peak memory: {PeakMemoryUsageMB:F2} MB");
		}

		/// <summary>
		/// Logs current performance metrics
		/// </summary>
		public void LogCurrentMetrics() {
			Logger.LogDebug($"Current metrics - Elapsed: {ElapsedTime.TotalSeconds:F2}s, Memory: {CurrentMemoryUsageMB:F2} MB");
		}
	}





	/// <summary>
	/// Throttles I/O operations based on system performance
	/// </summary>
	[System.Runtime.Versioning.SupportedOSPlatform("windows")]
	public class IoPerformanceThrottler {
		private readonly SemaphoreSlim _throttleSemaphore;
		private readonly PerformanceCounter? _diskReadCounter;
		private readonly PerformanceCounter? _diskWriteCounter;
		private readonly Int32 _maxConcurrentOperations;
		private readonly Double _ioThresholdMBPerSec;
		private readonly Object _lockObj = new Object();
		private DateTime _lastAdjustment = DateTime.Now;
		private Int32 _currentLimit;
		private Boolean _performanceCountersAvailable = true;

		/// <summary>
		/// Creates a new I/O performance throttler
		/// </summary>
		/// <param name="initialConcurrentOperations">Initial number of concurrent operations to allow</param>
		/// <param name="ioThresholdMBPerSec">I/O threshold in MB/s that triggers throttling adjustments</param>
		public IoPerformanceThrottler(
				Int32 initialConcurrentOperations = 4,
				Double ioThresholdMBPerSec = 80.0) {

			_maxConcurrentOperations = Math.Max(1, Environment.ProcessorCount);
			_ioThresholdMBPerSec = ioThresholdMBPerSec;
			_currentLimit = initialConcurrentOperations;
			_throttleSemaphore = new SemaphoreSlim(_currentLimit, _maxConcurrentOperations);

			Logger.LogInfo($"I/O Throttler initialized with {_currentLimit} concurrent operations (max: {_maxConcurrentOperations})");

			// Initialize performance counters for disk I/O monitoring
			try {
				_diskReadCounter = new PerformanceCounter("PhysicalDisk", "Disk Read Bytes/sec", "_Total");
				_diskWriteCounter = new PerformanceCounter("PhysicalDisk", "Disk Write Bytes/sec", "_Total");

				// Get initial values to ensure counters are working
				_diskReadCounter.NextValue();
				_diskWriteCounter.NextValue();

				Logger.LogInfo("Performance counters successfully initialized for I/O throttling");
			}
			catch (Exception ex) {
				Logger.LogWarning($"Could not initialize performance counters: {ex.Message}");
				Logger.LogWarning("I/O throttling will use static limits only");
				_performanceCountersAvailable = false;
			}
		}

		/// <summary>
		/// Gets throttled access for an I/O operation
		/// </summary>
		/// <returns>A disposable object that releases the throttle when disposed</returns>
		public async Task<IDisposable> GetThrottledAccessAsync() {
			// Check if we need to adjust throttling based on current I/O performance
			AdjustThrottleIfNeeded();

			// Wait until a slot becomes available
			await _throttleSemaphore.WaitAsync();
			return new SemaphoreReleaser(_throttleSemaphore);
		}

		/// <summary>
		/// Adjusts the throttling level based on current system I/O performance
		/// </summary>
		private void AdjustThrottleIfNeeded() {
			// Only check every 5 seconds to avoid too frequent adjustments
			if ((DateTime.Now - _lastAdjustment).TotalSeconds < 5)
				return;

			lock (_lockObj) {
				if ((DateTime.Now - _lastAdjustment).TotalSeconds < 5)
					return;

				_lastAdjustment = DateTime.Now;

				try {
					// Skip if performance counters aren't available
					if (!_performanceCountersAvailable || _diskReadCounter == null || _diskWriteCounter == null)
						return;

					// Get current disk I/O in MB/s
					Double readMBPerSec = _diskReadCounter.NextValue() / (1024 * 1024);
					Double writeMBPerSec = _diskWriteCounter.NextValue() / (1024 * 1024);
					Double totalMBPerSec = readMBPerSec + writeMBPerSec;

					Logger.LogDebug($"Current disk I/O: {totalMBPerSec:F2} MB/s (Read: {readMBPerSec:F2}, Write: {writeMBPerSec:F2})");

					Int32 newLimit = _currentLimit;

					// Adjust limit based on I/O performance
					if (totalMBPerSec > _ioThresholdMBPerSec * 1.5) {
						// Too much I/O, decrease concurrency
						newLimit = Math.Max(1, _currentLimit - 1);
						Logger.LogDebug($"High I/O detected ({totalMBPerSec:F2} MB/s), decreasing concurrency");
					}
					else if (totalMBPerSec < _ioThresholdMBPerSec * 0.5 && _currentLimit < _maxConcurrentOperations) {
						// I/O capacity available, increase concurrency
						newLimit = Math.Min(_maxConcurrentOperations, _currentLimit + 1);
						Logger.LogDebug($"Low I/O detected ({totalMBPerSec:F2} MB/s), increasing concurrency");
					}

					// Update the semaphore if the limit changed
					if (newLimit != _currentLimit) {
						Int32 difference = newLimit - _currentLimit;
						_currentLimit = newLimit;

						if (difference > 0) {
							_throttleSemaphore.Release(difference);
							Logger.LogInfo($"Increased I/O concurrency to {_currentLimit} (Disk I/O: {totalMBPerSec:F2} MB/s)");
						}
						else if (difference < 0) {
							// We can't reduce the semaphore count directly
							// The count will gradually decrease as tasks complete
							// and new ones will be throttled by the adjusted max count
							Logger.LogInfo($"Decreased I/O concurrency target to {_currentLimit} (Disk I/O: {totalMBPerSec:F2} MB/s)");
						}
					}
				}
				catch (Exception ex) {
					Logger.LogWarning($"Error in throttle adjustment: {ex.Message}");
					_performanceCountersAvailable = false;
				}
			}
		}

		/// <summary>
		/// Helper class that releases the semaphore when disposed
		/// </summary>
		private class SemaphoreReleaser : IDisposable {
			private readonly SemaphoreSlim _semaphore;
			private Boolean _disposed = false;

			public SemaphoreReleaser(SemaphoreSlim semaphore) {
				_semaphore = semaphore;
			}

			public void Dispose() {
				if (!_disposed) {
					_semaphore.Release();
					_disposed = true;
				}
			}
		}
	}


}