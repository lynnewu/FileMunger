using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks.Dataflow;

using FileMunger.modules;



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

		/// <summary>
		/// Entry point for the FileMunger application.
		/// </summary>
		/// <param name="args">Command line arguments</param>
		static async Task Main(String[] args) {
			// Parse command line arguments
			CommandLineOptions options = ParseCommandLineArguments(args);

			// Check if options are valid
			if (!options.IsValid) {
				Console.WriteLine($"Error: {options.ErrorMessage}");
				PrintUsage();
				return;
			}

			// Initialize logging with the specified verbosity level
			Logger.Initialize(options.Verbosity);
			Logger.LogInfo("FileMunger starting up...");
			Logger.LogDebug($"Running on .NET version: {Environment.Version}");

			// Log the options
			Logger.LogInfo($"Options: Recursive={options.Recursive}, FileSpec={options.FileSpec}, Verbosity={options.Verbosity}");
			//foreach (String dir in options.Directories) {
			//	Logger.LogInfo($"Directory to process: {dir}");
			//}


			// Create performance tracker
			PerformanceTracker performanceTracker = new PerformanceTracker();
			performanceTracker.Start();

			// Track global statistics
			ConcurrentDictionary<String, Boolean> globalVisitedPaths = new ConcurrentDictionary<String, Boolean>();
			Int64 totalFilesProcessed = 0;
			Int64 totalBytesProcessed = 0;
			Int64 totalErrorsEncountered = 0;
			Int64 totalDirectoriesProcessed = 0;

			try {
				// Get system information for optimizing parallelism
				Int32 processorCount = Environment.ProcessorCount;
				Logger.LogInfo($"System has {processorCount} logical processors");

				// High performance settings - significantly increase parallelism
				Int32 maxConcurrentOperations = processorCount * 4;
				Int32 fileQueueSize = processorCount * 10;

				// Create a custom IoPerformanceThrottler with appropriate concurrency limits
				IoPerformanceThrottler highPerfThrottler = new IoPerformanceThrottler(
						initialConcurrentOperations: Math.Min(maxConcurrentOperations / 2, Environment.ProcessorCount * 3),
						ioThresholdMBPerSec: 500.0
				);

				// Create a block for file processing with throttling
				ExecutionDataflowBlockOptions blockOptions = new ExecutionDataflowBlockOptions {
					MaxDegreeOfParallelism = maxConcurrentOperations,
					BoundedCapacity = fileQueueSize,
					EnsureOrdered = false
				};

				// Create an ActionBlock that will process files in parallel
				ActionBlock<String> fileProcessor = new ActionBlock<String>(
						async filePath => {
							try {
								// Process file associations
								GetAssociations.ProcessFile(filePath);

								// Process each file and track statistics
								FileInfo fileInfo = new FileInfo(filePath);
								FileProcessingResult result = await ProcessFileAsync(filePath, highPerfThrottler);

								if (result.Success) {
									Interlocked.Increment(ref totalFilesProcessed);
									Interlocked.Add(ref totalBytesProcessed, fileInfo.Length);

									// Periodically log progress for medium+ verbosity
									if (options.Verbosity <= LogLevel.Info && totalFilesProcessed % 10000 == 0) {
										Logger.LogInfo($"Progress: {totalFilesProcessed:N0} files ({totalBytesProcessed / (1024.0 * 1024.0):N2} MB)");
									}
								}
								else {
									Interlocked.Increment(ref totalErrorsEncountered);
								}
							}
							catch (Exception ex) {
								Logger.LogError($"Error in file processor: {ex.Message}");
								Interlocked.Increment(ref totalErrorsEncountered);
							}
						},
						blockOptions
				);

				// Process each directory
				List<Task> directoryTasks = new List<Task>();

				foreach (String directory in options.Directories) {
					directoryTasks.Add(Task.Run(async () => {
						Logger.LogInfo($"Starting processing of directory: {directory}");
						await ProcessDirectoryAsync(directory, fileProcessor, options.Recursive, options.FileSpec, globalVisitedPaths, ref totalDirectoriesProcessed);
						Logger.LogInfo($"Completed processing of directory: {directory}");
					}));
				}

				// Wait for all directory processing tasks to complete
				await Task.WhenAll(directoryTasks);

				// Complete the processor and wait for all queued files to finish processing
				Logger.LogInfo("All directories traversed, waiting for file processing to complete...");
				fileProcessor.Complete();
				await fileProcessor.Completion;

				// Log final statistics
				performanceTracker.Stop();
				Double processingTimeSeconds = performanceTracker.ElapsedTime.TotalSeconds;
				Logger.LogInfo($"Processing complete in {processingTimeSeconds:F2} seconds");
				Logger.LogInfo($"Directories processed: {totalDirectoriesProcessed:N0}");
				Logger.LogInfo($"Files processed: {totalFilesProcessed:N0}");
				Logger.LogInfo($"Total data: {(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB");
				Logger.LogInfo($"Errors encountered: {totalErrorsEncountered:N0}");
				Logger.LogInfo($"Peak memory usage: {performanceTracker.PeakMemoryUsageMB:F2} MB");

				// Calculate and log performance metrics
				if (processingTimeSeconds > 0) {
					Double filesPerSecond = totalFilesProcessed / processingTimeSeconds;
					Double mbPerSecond = (totalBytesProcessed / (1024.0 * 1024.0)) / processingTimeSeconds;
					Logger.LogInfo($"Performance: {filesPerSecond:F2} files/sec, {mbPerSecond:F2} MB/sec");
				}

				// Print the file association results
				if (GetAssociations.UniqueExtensionsCount > 0) {
					String results = GetAssociations.PrintResultsTable();

					// Output to console for non-quiet verbosity levels
					if (options.Verbosity < LogLevel.Error) {
						Console.WriteLine();
						Console.WriteLine(results);
					}

					// Always save to a file
					File.WriteAllText("FileAssociations.txt", results);
					Logger.LogInfo($"File associations written to FileAssociations.txt");
				}
			}
			catch (Exception ex) {
				Logger.LogError($"Un!processed exception in main processing: {ex}");
				Console.WriteLine("An error occurred during processing. Check logs for details.");
			}
			finally {
				// Clean up resources
				GetAssociations.Cleanup();
			}

			// Final output message
			if (options.Verbosity < LogLevel.Error) {
				Console.WriteLine($"Processing complete! Processed {totalFilesProcessed:N0} files ({(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB)");
			}
		}

		/// <summary>
		/// Parses command line arguments and returns options
		/// </summary>
		/// <param name="args">Command line arguments</param>
		/// <returns>Parsed options</returns>
		private static CommandLineOptions ParseCommandLineArguments(String[] args) {
			CommandLineOptions options = new CommandLineOptions();

			try {
				// Default to current drive root if no directories specified
				Boolean directoriesSpecified = false;

				// Process each argument
				for (Int32 i = 0; i < args.Length; i++) {
					String arg = args[i].ToLowerInvariant();

					// Check for named arguments
					if (arg.StartsWith("--") || arg.StartsWith("-")) {
						String argName = arg.TrimStart('-').ToLowerInvariant();

						// Get the value (if any)
						String? value = null;
						if (i + 1 < args.Length && !args[i + 1].StartsWith("-")) {
							value = args[++i];
						}

						switch (argName) {
							case "directories":
							case "dir":
							case "d":
								if (String.IsNullOrEmpty(value)) {
									options.IsValid = false;
									options.ErrorMessage = "Directories argument requires a value";
									return options;
								}

								directoriesSpecified = true;
								options.Directories = ParseDirectoriesArgument(value);
								break;

							case "recursive":
							case "r":
								if (!String.IsNullOrEmpty(value)) {
									options.Recursive = ParseBooleanArgument(value);
								}
								break;

							case "filespec":
							case "f":
								if (String.IsNullOrEmpty(value)) {
									options.IsValid = false;
									options.ErrorMessage = "FileSpec argument requires a value";
									return options;
								}

								options.FileSpec = value;
								break;

							case "verbosity":
							case "v":
								if (String.IsNullOrEmpty(value)) {
									options.IsValid = false;
									options.ErrorMessage = "Verbosity argument requires a value";
									return options;
								}

								options.Verbosity = ParseVerbosityArgument(value);
								break;

							case "help":
							case "h":
							case "?":
								options.IsValid = false;
								options.ErrorMessage = "Help requested";
								return options;

							default:
								options.IsValid = false;
								options.ErrorMessage = $"Unknown argument: {arg}";
								return options;
						}
					}
					// If not a named argument, treat as directory
					else {
						directoriesSpecified = true;
						options.Directories.Add(arg);
					}
				}

				// If no directories were specified, use defaults
				if (!directoriesSpecified) {
					options.Directories = GetDefaultDirectories();
				}

				// Validate the directories
				List<String> validDirectories = new List<String>();
				foreach (String dir in options.Directories) {
					if (Directory.Exists(dir)) {
						validDirectories.Add(dir);
					}
				}

				if (validDirectories.Count == 0) {
					options.IsValid = false;
					options.ErrorMessage = "No valid directories to process";
					return options;
				}

				options.Directories = validDirectories;
			}
			catch (Exception ex) {
				options.IsValid = false;
				options.ErrorMessage = $"Error parsing arguments: {ex.Message}";
			}

			return options;
		}

		/// <summary>
		/// Parses the directories argument, handling special values like 'all'
		/// </summary>
		/// <param name="directoriesArg">The directories argument string</param>
		/// <returns>A list of directory paths to process</returns>
		private static List<String> ParseDirectoriesArgument(String directoriesArg) {
			List<String> result = new List<String>();

			// Check for special 'all' value to include all drive roots
			if (directoriesArg.Equals("all", StringComparison.OrdinalIgnoreCase)) {
				foreach (DriveInfo drive in DriveInfo.GetDrives()) {
					// Only include ready local drives (not network, removable, etc.)
					if (drive.IsReady && drive.DriveType == DriveType.Fixed) {
						result.Add(drive.RootDirectory.FullName);
					}
				}
			}
			else {
				// Split by comma and add each directory
				String[] dirs = directoriesArg.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
				result.AddRange(dirs);
			}

			return result;
		}

		/// <summary>
		/// Gets the default directories to process
		/// </summary>
		private static List<String> GetDefaultDirectories() {
			List<String> result = new List<String>();

			// Default to the current drive's root directory
			String currentDrive = Path.GetPathRoot(Environment.CurrentDirectory) ?? "C:\\";
			result.Add(currentDrive);

			return result;
		}

		/// <summary>
		/// Parses a boolean argument value
		/// </summary>
		/// <param name="value">The argument value to parse</param>
		/// <returns>The boolean value</returns>
		private static Boolean ParseBooleanArgument(String value) {
			return value.ToLowerInvariant() switch {
				"yes" or "y" or "true" or "t" or "1" or "on" => true,
				"no" or "n" or "false" or "f" or "0" or "off" => false,
				_ => true // Default to true for unrecognized values
			};
		}

		/// <summary>
		/// Parses a verbosity argument value
		/// </summary>
		/// <param name="value">The argument value to parse</param>
		/// <returns>The log level</returns>
		private static LogLevel ParseVerbosityArgument(String value) {
			return value.ToLowerInvariant() switch {
				"high" or "debug" or "verbose" => LogLevel.Debug,
				"medium" or "info" => LogLevel.Info,
				"low" or "warning" or "warn" => LogLevel.Warning,
				"none" or "error" or "quiet" => LogLevel.Error,
				_ => LogLevel.Info // Default to info for unrecognized values
			};
		}

		/// <summary>
		/// Prints usage information
		/// </summary>
		private static void PrintUsage() {
			Console.WriteLine("FileMunger - Advanced file system processor");
			Console.WriteLine();
			Console.WriteLine("Usage: FileMunger [options]");
			Console.WriteLine();
			Console.WriteLine("Options:");
			Console.WriteLine("  --directories, -dir, -d <dirs>    Comma-separated list of directories to process");
			Console.WriteLine("                                    Special value 'all' processes all local drive roots");
			Console.WriteLine("                                    Default: current drive root");
			Console.WriteLine();
			Console.WriteLine("  --recursive, -r [yes|no]          Process subdirectories recursively");
			Console.WriteLine("                                    Default: yes");
			Console.WriteLine();
			Console.WriteLine("  --filespec, -f <pattern>          File specification pattern (Windows wildcards)");
			Console.WriteLine("                                    Default: *.*");
			Console.WriteLine();
			Console.WriteLine("  --verbosity, -v <level>           Output verbosity level");
			Console.WriteLine("                                    Values: high, medium, low, none");
			Console.WriteLine("                                    Default: low");
			Console.WriteLine();
			Console.WriteLine("  --help, -h, -?                    Show this help message");
			Console.WriteLine();
			Console.WriteLine("Examples:");
			Console.WriteLine("  FileMunger --directories C:\\Data,D:\\Backup --filespec *.docx");
			Console.WriteLine("  FileMunger -d all -r no -v high");
			Console.WriteLine("  FileMunger C:\\Data");
		}

		/// <summary>
		/// Processes a directory and its files, with optional recursion
		/// </summary>
		private static async Task ProcessDirectoryAsync(
				String rootDirectory,
				ActionBlock<String> fileProcessor,
				Boolean recursive,
				String fileSpec,
				ConcurrentDictionary<String, Boolean> visitedPaths,
				ref Int64 directoryCount) {

			try {
				Logger.LogDebug($"Processing directory: {rootDirectory}");

				// Create a queue for breadth-first directory traversal if recursive
				Queue<String> directoriesToProcess = new Queue<String>();
				directoriesToProcess.Enqueue(rootDirectory);

				while (directoriesToProcess.Count > 0) {
					String currentDir = directoriesToProcess.Dequeue();
					Interlocked.Increment(ref directoryCount);

					try {
						// Process matching files in the current directory
						String searchPattern = fileSpec;
						foreach (String filePath in Directory.EnumerateFiles(currentDir, searchPattern)) {
							try {
								FileInfo fileInfo = new FileInfo(filePath);

								// Handle symbolic links to avoid cycles
								if ((fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
									String? targetPath = ResolveSymbolicLink(filePath);

									if (!String.IsNullOrEmpty(targetPath)) {
										if (!visitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
											Logger.LogDebug($"Skipping previously visited link: {filePath} -> {targetPath}");
											continue;
										}
									}
								}

								// Queue the file for processing
								Boolean accepted = await fileProcessor.SendAsync(filePath);
								if (!accepted) {
									// If the queue is full, wait and retry
									Int32 retryCount = 0;
									while (!accepted && retryCount < 10) {
										await Task.Delay(50 * (retryCount + 1));
										accepted = await fileProcessor.SendAsync(filePath);
										retryCount++;
									}

									if (!accepted) {
										Logger.LogWarning($"Failed to queue file after retries: {filePath}");
									}
								}
							}
							catch (Exception ex) {
								Logger.LogWarning($"Error processing file {filePath}: {ex.Message}");
							}
						}

						// Process subdirectories if recursive
						if (recursive) {
							foreach (String dirPath in Directory.EnumerateDirectories(currentDir)) {
								try {
									DirectoryInfo dirInfo = new DirectoryInfo(dirPath);

									// Skip system directories
									if ((dirInfo.Attributes & FileAttributes.System) == FileAttributes.System) {
										Logger.LogDebug($"Skipping system directory: {dirPath}");
										continue;
									}

									// Handle symbolic links to avoid cycles
									if ((dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
										String? targetPath = ResolveSymbolicLink(dirPath);

										if (!String.IsNullOrEmpty(targetPath)) {
											if (!visitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
												Logger.LogDebug($"Skipping previously visited directory link: {dirPath} -> {targetPath}");
												continue;
											}

											// Use target path instead
											directoriesToProcess.Enqueue(targetPath);
											continue;
										}
									}

									// Queue regular directory for processing
									directoriesToProcess.Enqueue(dirPath);
								}
								catch (UnauthorizedAccessException) {
									Logger.LogWarning($"Access denied to directory: {dirPath}");
								}
								catch (Exception ex) {
									Logger.LogWarning($"Error accessing directory {dirPath}: {ex.Message}");
								}
							}
						}
					}
					catch (UnauthorizedAccessException) {
						Logger.LogWarning($"Access denied to directory: {currentDir}");
					}
					catch (Exception ex) {
						Logger.LogWarning($"Error processing directory {currentDir}: {ex.Message}");
					}
				}
			}
			catch (Exception ex) {
				Logger.LogError($"Error in directory processing: {ex.Message}");
			}
		}

		///// <summary>
		///// Entry point for the FileMunger application.
		///// </summary>
		///// <param name="args">Command line arguments. First argument contains comma-separated directory paths to process.</param>
		//static async Task Main(String[] args) {
		//	// Initialize logging with minimal logging for performance
		//	Logger.Initialize(LogLevel.Info);
		//	Logger.LogInfo("FileMunger starting up with high-performance parallel configuration");

		//	// Validate command line arguments
		//	if (args.Length == 0) {
		//		Logger.LogError("No directory paths provided. Usage: FileMunger.exe [directory_path1,directory_path2,...]");
		//		Console.WriteLine("Please provide at least one directory path to scan (separate multiple paths with commas)");
		//		return;
		//	}

		//	// Split the comma-separated paths
		//	String[] directoryPaths = args[0].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

		//	if (directoryPaths.Length == 0) {
		//		Logger.LogError("No valid directory paths found after splitting");
		//		Console.WriteLine("Please provide valid directory paths separated by commas");
		//		return;
		//	}

		//	// Verify each directory exists
		//	List<String> validDirectories = directoryPaths.Where(Directory.Exists).ToList();

		//	if (validDirectories.Count == 0) {
		//		Logger.LogError("No valid directories to process");
		//		Console.WriteLine("No valid directories found to process");
		//		return;
		//	}

		//	// Get system information for optimizing parallelism
		//	Int32 processorCount = Environment.ProcessorCount;
		//	Int64 availableMemoryMB = GetAvailableMemoryMB();

		//	Logger.LogInfo($"System has {processorCount} logical processors and {availableMemoryMB} MB available memory");

		//	// High performance settings - significantly increase parallelism
		//	Int32 maxConcurrentOperations = processorCount * 8;  // Much higher concurrency
		//	Int32 fileQueueSize = processorCount * 20; // Larger queue to keep CPU fed
		//	Int32 directoryParallelism = Math.Max(8, processorCount / 2); // Process multiple directories simultaneously
		//	Int32 enumBatchSize = 1000; // Batch file enumeration for better performance

		//	Logger.LogInfo($"Configuring for high parallelism: {maxConcurrentOperations} concurrent operations, {fileQueueSize} queue size");

		//	// Create performance tracker
		//	PerformanceTracker performanceTracker = new PerformanceTracker();
		//	performanceTracker.Start();

		//	// Track global statistics
		//	ConcurrentDictionary<String, Boolean> globalVisitedPaths = new ConcurrentDictionary<String, Boolean>();
		//	Int64 totalFilesProcessed = 0;
		//	Int64 totalBytesProcessed = 0;
		//	Int64 totalErrorsEncountered = 0;
		//	Int64 totalDirectoriesProcessed = 0;

		//	try {
		//		// Create a single ActionBlock for file processing with a very high degree of parallelism
		//		// This allows files from any directory to be processed as capacity is available
		//		ExecutionDataflowBlockOptions blockOptions = new ExecutionDataflowBlockOptions {
		//			MaxDegreeOfParallelism = maxConcurrentOperations,
		//			BoundedCapacity = fileQueueSize,
		//			EnsureOrdered = false,
		//			SingleProducerConstrained = false
		//		};


		//		// Create a custom IoPerformanceThrottler with higher concurrency limits
		//		IoPerformanceThrottler highPerfThrottler = new IoPerformanceThrottler(
		//				initialConcurrentOperations: Math.Min(maxConcurrentOperations / 2, Environment.ProcessorCount * 3),  // Start with a safer value
		//				ioThresholdMBPerSec: 2500.0  // Set a high threshold to encourage max throughput
		//		);


		//		// Create an ActionBlock that will process files in parallel
		//		ActionBlock<String> globalFileProcessor = new ActionBlock<String>(
		//				async filePath => {
		//					try {
		//						// Process each file and track statistics
		//						FileInfo fileInfo = new FileInfo(filePath);
		//						FileProcessingResult result = await ProcessFileAsync(filePath, highPerfThrottler);

		//						if (result.Success) {
		//							Interlocked.Increment(ref totalFilesProcessed);
		//							Interlocked.Add(ref totalBytesProcessed, fileInfo.Length);

		//							// Periodically log progress
		//							if (totalFilesProcessed % 10000 == 0) {
		//								Logger.LogInfo($"Progress: {totalFilesProcessed:N0} files ({(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB), {totalDirectoriesProcessed:N0} directories");
		//							}
		//						}
		//						else {
		//							Interlocked.Increment(ref totalErrorsEncountered);
		//						}
		//					}
		//					catch (Exception ex) {
		//						Logger.LogError($"Error in file processor: {ex.Message}");
		//						Interlocked.Increment(ref totalErrorsEncountered);
		//					}
		//				},
		//				blockOptions
		//		);

		//		// Process directories in parallel
		//		List<Task> directoryTasks = new List<Task>();
		//		SemaphoreSlim directorySemaphore = new SemaphoreSlim(directoryParallelism);

		//		foreach (String rootDir in validDirectories) {
		//			// Throttle the number of concurrent directory traversals
		//			await directorySemaphore.WaitAsync();

		//			// Process this directory asynchronously
		//			Task directoryTask = Task.Run(async () => {
		//				try {
		//					Logger.LogInfo($"Starting directory traversal: {rootDir}");

		//					// Use a queue for breadth-first directory traversal
		//					ConcurrentQueue<String> directoriesToProcess = new ConcurrentQueue<String>();
		//					directoriesToProcess.Enqueue(rootDir);

		//					List<Task> workerTasks = new List<Task>();
		//					SemaphoreSlim workerSemaphore = new SemaphoreSlim(directoryParallelism * 2);

		//					// Start multiple worker tasks to process directories in parallel
		//					for (Int32 i = 0; i < directoryParallelism; i++) {
		//						workerTasks.Add(Task.Run(async () => {
		//							String? currentDir;

		//							// Process directories until the queue is empty
		//							//									while (directoriesToProcess.TryDequeue(out currentDir) || !IsQueueEmpty()) {
		//							while (directoriesToProcess.TryDequeue(out currentDir) || !IsQueueEmpty()) {
		//								if (currentDir == null) {
		//									// If we didn't get a directory, check if other workers are still active
		//									await Task.Delay(1);
		//									continue;
		//								}
		//								if (currentDir == null) {
		//									// If we didn't get a directory, check if other workers are still active
		//									await Task.Delay(10);
		//									continue;
		//								}

		//								Interlocked.Increment(ref totalDirectoriesProcessed);

		//								try {
		//									// Process files in batches for better performance
		//									List<String> fileBatch = new List<String>(enumBatchSize);

		//									// Find all files in the current directory
		//									foreach (String filePath in Directory.EnumerateFiles(currentDir)) {
		//										try {
		//											FileInfo fileInfo = new FileInfo(filePath);

		//											// Handle symbolic links to avoid cycles
		//											if ((fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
		//												String? targetPath = ResolveSymbolicLink(filePath);

		//												if (!String.IsNullOrEmpty(targetPath)) {
		//													if (!globalVisitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
		//														continue; // Skip already visited links
		//													}
		//												}
		//											}

		//											// Add to the batch
		//											fileBatch.Add(filePath);

		//											// When batch is full, send to processor
		//											if (fileBatch.Count >= enumBatchSize) {
		//												await SendFileBatchAsync(fileBatch, globalFileProcessor);
		//												fileBatch = new List<String>(enumBatchSize);
		//											}
		//										}
		//										catch (Exception) {
		//											Interlocked.Increment(ref totalErrorsEncountered);
		//											// Continue with next file
		//										}
		//									}

		//									// Send any remaining files in the batch
		//									if (fileBatch.Count > 0) {
		//										await SendFileBatchAsync(fileBatch, globalFileProcessor);
		//									}

		//									// Queue all subdirectories for processing
		//									foreach (String dirPath in Directory.EnumerateDirectories(currentDir)) {
		//										try {
		//											DirectoryInfo dirInfo = new DirectoryInfo(dirPath);

		//											// Handle directory symbolic links
		//											if ((dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
		//												String? targetPath = ResolveSymbolicLink(dirPath);

		//												if (!String.IsNullOrEmpty(targetPath)) {
		//													if (!globalVisitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
		//														continue; // Skip already visited links
		//													}
		//													directoriesToProcess.Enqueue(targetPath);
		//												}
		//												continue;
		//											}

		//											// Queue regular directories for processing
		//											directoriesToProcess.Enqueue(dirPath);
		//										}
		//										catch (UnauthorizedAccessException) {
		//											// Skip inaccessible directories
		//											Interlocked.Increment(ref totalErrorsEncountered);
		//										}
		//										catch (Exception) {
		//											Interlocked.Increment(ref totalErrorsEncountered);
		//										}
		//									}
		//								}
		//								catch (UnauthorizedAccessException) {
		//									// Skip inaccessible directories
		//									Interlocked.Increment(ref totalErrorsEncountered);
		//								}
		//								catch (Exception) {
		//									Interlocked.Increment(ref totalErrorsEncountered);
		//								}
		//							}
		//						}));
		//					}

		//					// Helper function to check if the queue is completely empty
		//					Boolean IsQueueEmpty() {
		//						return directoriesToProcess.IsEmpty;
		//					}

		//					// Wait for all worker tasks to finish
		//					await Task.WhenAll(workerTasks);

		//					Logger.LogInfo($"Completed directory traversal: {rootDir}");
		//				}
		//				catch (Exception ex) {
		//					Logger.LogError($"Error processing directory tree {rootDir}: {ex.Message}");
		//				}
		//				finally {
		//					// Release the semaphore to allow processing another root directory
		//					directorySemaphore.Release();
		//				}
		//			});

		//			directoryTasks.Add(directoryTask);
		//		}

		//		// Helper method to send batches of files to the processor
		//		async Task SendFileBatchAsync(List<String> fileBatch, ActionBlock<String> processor) {
		//			foreach (String filePath in fileBatch) {
		//				// Keep trying to send files, with backoff if the queue is full
		//				Boolean accepted = false;
		//				Int32 retryCount = 0;

		//				while (!accepted) {
		//					accepted = await processor.SendAsync(filePath);

		//					if (!accepted) {
		//						retryCount++;
		//						Int32 delayMs = Math.Min(100 * retryCount, 1000);
		//						await Task.Delay(delayMs);
		//					}
		//				}
		//			}
		//		}

		//		// Wait for all directory tasks to complete
		//		await Task.WhenAll(directoryTasks);

		//		// Complete the processor and wait for all queued files to finish processing
		//		Logger.LogInfo("All directories traversed, waiting for file processing to complete...");
		//		globalFileProcessor.Complete();
		//		await globalFileProcessor.Completion;

		//		// Log final statistics
		//		performanceTracker.Stop();
		//		Logger.LogInfo($"Processing complete in {performanceTracker.ElapsedTime.TotalSeconds:F2} seconds");
		//		Logger.LogInfo($"Directories processed: {totalDirectoriesProcessed:N0}");
		//		Logger.LogInfo($"Files processed: {totalFilesProcessed:N0}");
		//		Logger.LogInfo($"Total data: {(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB");
		//		Logger.LogInfo($"Errors encountered: {totalErrorsEncountered:N0}");
		//		Logger.LogInfo($"Peak memory usage: {performanceTracker.PeakMemoryUsageMB:F2} MB");

		//		// Calculate performance metrics
		//		Double filesPerSecond = totalFilesProcessed / performanceTracker.ElapsedTime.TotalSeconds;
		//		Double mbPerSecond = (totalBytesProcessed / (1024.0 * 1024.0)) / performanceTracker.ElapsedTime.TotalSeconds;

		//		Logger.LogInfo($"Performance: {filesPerSecond:F2} files/sec, {mbPerSecond:F2} MB/sec");
		//	}
		//	catch (Exception ex) {
		//		Logger.LogError($"Unhandled exception in main processing: {ex}");
		//		Console.WriteLine("An error occurred during processing. Check logs for details.");
		//	}

		//	Console.WriteLine($"Processing complete! Processed {totalFilesProcessed:N0} files ({(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB)");
		//}

		/// <summary>
		/// Gets the available memory in megabytes
		/// </summary>
		private static Int64 GetAvailableMemoryMB() {
			try {
				using (Process process = Process.GetCurrentProcess()) {
					return (GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (1024 * 1024));
				}
			}
			catch {
				return 8192; // Default to 8GB if we can't determine
			}
		}


		/// <summary>
		/// Processes a directory and its files, with optional recursion
		/// </summary>
		/// <returns>The number of directories processed</returns>
		private static async Task<Int64> ProcessDirectoryAsync(
				String rootDirectory,
				ActionBlock<String> fileProcessor,
				Boolean recursive,
				String fileSpec,
				ConcurrentDictionary<String, Boolean> visitedPaths) {

			Int64 localDirectoryCount = 0;

			try {
				Logger.LogDebug($"Processing directory: {rootDirectory}");

				// Create a queue for breadth-first directory traversal if recursive
				Queue<String> directoriesToProcess = new Queue<String>();
				directoriesToProcess.Enqueue(rootDirectory);

				while (directoriesToProcess.Count > 0) {
					String currentDir = directoriesToProcess.Dequeue();
					localDirectoryCount++;

					try {
						// Process matching files in the current directory
						String searchPattern = fileSpec;
						foreach (String filePath in Directory.EnumerateFiles(currentDir, searchPattern)) {
							try {
								FileInfo fileInfo = new FileInfo(filePath);

								// Handle symbolic links to avoid cycles
								if ((fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
									String? targetPath = ResolveSymbolicLink(filePath);

									if (!String.IsNullOrEmpty(targetPath)) {
										if (!visitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
											Logger.LogDebug($"Skipping previously visited link: {filePath} -> {targetPath}");
											continue;
										}
									}
								}

								// Queue the file for processing
								Boolean accepted = await fileProcessor.SendAsync(filePath);
								if (!accepted) {
									// If the queue is full, wait and retry
									Int32 retryCount = 0;
									while (!accepted && retryCount < 10) {
										await Task.Delay(50 * (retryCount + 1));
										accepted = await fileProcessor.SendAsync(filePath);
										retryCount++;
									}

									if (!accepted) {
										Logger.LogWarning($"Failed to queue file after retries: {filePath}");
									}
								}
							}
							catch (Exception ex) {
								Logger.LogWarning($"Error processing file {filePath}: {ex.Message}");
							}
						}

						// Process subdirectories if recursive
						if (recursive) {
							foreach (String dirPath in Directory.EnumerateDirectories(currentDir)) {
								try {
									DirectoryInfo dirInfo = new DirectoryInfo(dirPath);

									// Skip system directories
									if ((dirInfo.Attributes & FileAttributes.System) == FileAttributes.System) {
										Logger.LogDebug($"Skipping system directory: {dirPath}");
										continue;
									}

									// Handle symbolic links to avoid cycles
									if ((dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
										String? targetPath = ResolveSymbolicLink(dirPath);

										if (!String.IsNullOrEmpty(targetPath)) {
											if (!visitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
												Logger.LogDebug($"Skipping previously visited directory link: {dirPath} -> {targetPath}");
												continue;
											}

											// Use target path instead
											directoriesToProcess.Enqueue(targetPath);
											continue;
										}
									}

									// Queue regular directory for processing
									directoriesToProcess.Enqueue(dirPath);
								}
								catch (UnauthorizedAccessException) {
									Logger.LogWarning($"Access denied to directory: {dirPath}");
								}
								catch (Exception ex) {
									Logger.LogWarning($"Error accessing directory {dirPath}: {ex.Message}");
								}
							}
						}
					}
					catch (UnauthorizedAccessException) {
						Logger.LogWarning($"Access denied to directory: {currentDir}");
					}
					catch (Exception ex) {
						Logger.LogWarning($"Error processing directory {currentDir}: {ex.Message}");
					}
				}
			}
			catch (Exception ex) {
				Logger.LogError($"Error in directory processing: {ex.Message}");
			}

			return localDirectoryCount;
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
			catch (Exception) { }
			//catch (FileNotFoundException) {
			//	Logger.LogWarning($"File not found (may have been deleted): {filePath}");
			//	result.Success = false;
			//	result.ErrorMessage = "File not found";
			//}
			//catch (UnauthorizedAccessException) {
			//	Logger.LogWarning($"Access denied to file: {filePath}");
			//	result.Success = false;
			//	result.ErrorMessage = "Access denied";
			//}
			//catch (IOException ex) {
			//	Logger.LogWarning($"I/O error for file {filePath}: {ex.Message}");
			//	result.Success = false;
			//	result.ErrorMessage = $"I/O error: {ex.Message}";
			//}
			//catch (Exception ex) {
			//	Logger.LogError($"Unexpected error processing file {filePath}: {ex.Message}");
			//	result.Success = false;
			//	result.ErrorMessage = $"Error: {ex.Message}";
			//}

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
				String? targetPath = NativeMethods.GetFinalPathName(linkPath);
				Logger.LogDebug($"Link target: {targetPath}");
				return targetPath;
			}
			catch (Exception ex) {
				Logger.LogWarning($"Failed to resolve symbolic link {linkPath}: {ex.Message}");
				return null;
			}
		}
	}



}