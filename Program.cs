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
		/// <param name="args">Command line arguments. First argument contains comma-separated directory paths to process.</param>
		static async Task Main(String[] args) {
			// Initialize logging with minimal logging for performance
			Logger.Initialize(LogLevel.Info);
			Logger.LogInfo("FileMunger starting up with high-performance parallel configuration");

			// Validate command line arguments
			if (args.Length == 0) {
				Logger.LogError("No directory paths provided. Usage: FileMunger.exe [directory_path1,directory_path2,...]");
				Console.WriteLine("Please provide at least one directory path to scan (separate multiple paths with commas)");
				return;
			}

			// Split the comma-separated paths
			String[] directoryPaths = args[0].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

			if (directoryPaths.Length == 0) {
				Logger.LogError("No valid directory paths found after splitting");
				Console.WriteLine("Please provide valid directory paths separated by commas");
				return;
			}

			// Verify each directory exists
			List<String> validDirectories = directoryPaths.Where(Directory.Exists).ToList();

			if (validDirectories.Count == 0) {
				Logger.LogError("No valid directories to process");
				Console.WriteLine("No valid directories found to process");
				return;
			}

			// Get system information for optimizing parallelism
			Int32 processorCount = Environment.ProcessorCount;
			Int64 availableMemoryMB = GetAvailableMemoryMB();

			Logger.LogInfo($"System has {processorCount} logical processors and {availableMemoryMB} MB available memory");

			// High performance settings - significantly increase parallelism
			Int32 maxConcurrentOperations = processorCount * 8;  // Much higher concurrency
			Int32 fileQueueSize = processorCount * 20; // Larger queue to keep CPU fed
			Int32 directoryParallelism = Math.Max(8, processorCount / 2); // Process multiple directories simultaneously
			Int32 enumBatchSize = 1000; // Batch file enumeration for better performance

			Logger.LogInfo($"Configuring for high parallelism: {maxConcurrentOperations} concurrent operations, {fileQueueSize} queue size");

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
				// Create a single ActionBlock for file processing with a very high degree of parallelism
				// This allows files from any directory to be processed as capacity is available
				ExecutionDataflowBlockOptions blockOptions = new ExecutionDataflowBlockOptions {
					MaxDegreeOfParallelism = maxConcurrentOperations,
					BoundedCapacity = fileQueueSize,
					EnsureOrdered = false,
					SingleProducerConstrained = false
				};


				// Create a custom IoPerformanceThrottler with higher concurrency limits
				IoPerformanceThrottler highPerfThrottler = new IoPerformanceThrottler(
						initialConcurrentOperations: Math.Min(maxConcurrentOperations / 2, Environment.ProcessorCount * 3),  // Start with a safer value
						ioThresholdMBPerSec:2500.0  // Set a high threshold to encourage max throughput
				);


				// Create an ActionBlock that will process files in parallel
				ActionBlock<String> globalFileProcessor = new ActionBlock<String>(
						async filePath => {
							try {
								// Process each file and track statistics
								FileInfo fileInfo = new FileInfo(filePath);
								FileProcessingResult result = await ProcessFileAsync(filePath, highPerfThrottler);

								if (result.Success) {
									Interlocked.Increment(ref totalFilesProcessed);
									Interlocked.Add(ref totalBytesProcessed, fileInfo.Length);

									// Periodically log progress
									if (totalFilesProcessed % 10000 == 0) {
										Logger.LogInfo($"Progress: {totalFilesProcessed:N0} files ({(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB), {totalDirectoriesProcessed:N0} directories");
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

				// Process directories in parallel
				List<Task> directoryTasks = new List<Task>();
				SemaphoreSlim directorySemaphore = new SemaphoreSlim(directoryParallelism);

				foreach (String rootDir in validDirectories) {
					// Throttle the number of concurrent directory traversals
					await directorySemaphore.WaitAsync();

					// Process this directory asynchronously
					Task directoryTask = Task.Run(async () => {
						try {
							Logger.LogInfo($"Starting directory traversal: {rootDir}");

							// Use a queue for breadth-first directory traversal
							ConcurrentQueue<String> directoriesToProcess = new ConcurrentQueue<String>();
							directoriesToProcess.Enqueue(rootDir);

							List<Task> workerTasks = new List<Task>();
							SemaphoreSlim workerSemaphore = new SemaphoreSlim(directoryParallelism * 2);

							// Start multiple worker tasks to process directories in parallel
							for (Int32 i = 0; i < directoryParallelism; i++) {
								workerTasks.Add(Task.Run(async () => {
									String currentDir;

									// Process directories until the queue is empty
									while (directoriesToProcess.TryDequeue(out currentDir) || !IsQueueEmpty()) {
										if (currentDir == null) {
											// If we didn't get a directory, check if other workers are still active
											await Task.Delay(10);
											continue;
										}

										Interlocked.Increment(ref totalDirectoriesProcessed);

										try {
											// Process files in batches for better performance
											List<String> fileBatch = new List<String>(enumBatchSize);

											// Find all files in the current directory
											foreach (String filePath in Directory.EnumerateFiles(currentDir)) {
												try {
													FileInfo fileInfo = new FileInfo(filePath);

													// Handle symbolic links to avoid cycles
													if ((fileInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
														String? targetPath = ResolveSymbolicLink(filePath);

														if (!String.IsNullOrEmpty(targetPath)) {
															if (!globalVisitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
																continue; // Skip already visited links
															}
														}
													}

													// Add to the batch
													fileBatch.Add(filePath);

													// When batch is full, send to processor
													if (fileBatch.Count >= enumBatchSize) {
														await SendFileBatchAsync(fileBatch, globalFileProcessor);
														fileBatch = new List<String>(enumBatchSize);
													}
												}
												catch (Exception ) {
													Interlocked.Increment(ref totalErrorsEncountered);
													// Continue with next file
												}
											}

											// Send any remaining files in the batch
											if (fileBatch.Count > 0) {
												await SendFileBatchAsync(fileBatch, globalFileProcessor);
											}

											// Queue all subdirectories for processing
											foreach (String dirPath in Directory.EnumerateDirectories(currentDir)) {
												try {
													DirectoryInfo dirInfo = new DirectoryInfo(dirPath);

													// Handle directory symbolic links
													if ((dirInfo.Attributes & FileAttributes.ReparsePoint) == FileAttributes.ReparsePoint) {
														String? targetPath = ResolveSymbolicLink(dirPath);

														if (!String.IsNullOrEmpty(targetPath)) {
															if (!globalVisitedPaths.TryAdd(targetPath.ToLowerInvariant(), true)) {
																continue; // Skip already visited links
															}
															directoriesToProcess.Enqueue(targetPath);
														}
														continue;
													}

													// Queue regular directories for processing
													directoriesToProcess.Enqueue(dirPath);
												}
												catch (UnauthorizedAccessException) {
													// Skip inaccessible directories
													Interlocked.Increment(ref totalErrorsEncountered);
												}
												catch (Exception ) {
													Interlocked.Increment(ref totalErrorsEncountered);
												}
											}
										}
										catch (UnauthorizedAccessException) {
											// Skip inaccessible directories
											Interlocked.Increment(ref totalErrorsEncountered);
										}
										catch (Exception ) {
											Interlocked.Increment(ref totalErrorsEncountered);
										}
									}
								}));
							}

							// Helper function to check if the queue is completely empty
							Boolean IsQueueEmpty() {
								return directoriesToProcess.IsEmpty;
							}

							// Wait for all worker tasks to finish
							await Task.WhenAll(workerTasks);

							Logger.LogInfo($"Completed directory traversal: {rootDir}");
						}
						catch (Exception ex) {
							Logger.LogError($"Error processing directory tree {rootDir}: {ex.Message}");
						}
						finally {
							// Release the semaphore to allow processing another root directory
							directorySemaphore.Release();
						}
					});

					directoryTasks.Add(directoryTask);
				}

				// Helper method to send batches of files to the processor
				async Task SendFileBatchAsync(List<String> fileBatch, ActionBlock<String> processor) {
					foreach (String filePath in fileBatch) {
						// Keep trying to send files, with backoff if the queue is full
						Boolean accepted = false;
						Int32 retryCount = 0;

						while (!accepted) {
							accepted = await processor.SendAsync(filePath);

							if (!accepted) {
								retryCount++;
								Int32 delayMs = Math.Min(100 * retryCount, 1000);
								await Task.Delay(delayMs);
							}
						}
					}
				}

				// Wait for all directory tasks to complete
				await Task.WhenAll(directoryTasks);

				// Complete the processor and wait for all queued files to finish processing
				Logger.LogInfo("All directories traversed, waiting for file processing to complete...");
				globalFileProcessor.Complete();
				await globalFileProcessor.Completion;

				// Log final statistics
				performanceTracker.Stop();
				Logger.LogInfo($"Processing complete in {performanceTracker.ElapsedTime.TotalSeconds:F2} seconds");
				Logger.LogInfo($"Directories processed: {totalDirectoriesProcessed:N0}");
				Logger.LogInfo($"Files processed: {totalFilesProcessed:N0}");
				Logger.LogInfo($"Total data: {(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB");
				Logger.LogInfo($"Errors encountered: {totalErrorsEncountered:N0}");
				Logger.LogInfo($"Peak memory usage: {performanceTracker.PeakMemoryUsageMB:F2} MB");

				// Calculate performance metrics
				Double filesPerSecond = totalFilesProcessed / performanceTracker.ElapsedTime.TotalSeconds;
				Double mbPerSecond = (totalBytesProcessed / (1024.0 * 1024.0)) / performanceTracker.ElapsedTime.TotalSeconds;

				Logger.LogInfo($"Performance: {filesPerSecond:F2} files/sec, {mbPerSecond:F2} MB/sec");
			}
			catch (Exception ex) {
				Logger.LogError($"Unhandled exception in main processing: {ex}");
				Console.WriteLine("An error occurred during processing. Check logs for details.");
			}

			Console.WriteLine($"Processing complete! Processed {totalFilesProcessed:N0} files ({(totalBytesProcessed / (1024.0 * 1024.0)):N2} MB)");
		}

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

	/// <summary>
	/// Result of a file processing operation.
	/// </summary>
	public class FileProcessingResult {
		public String? FilePath { get; set; } = String.Empty; // Initialize with empty string
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

		[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
		private static extern Int32 FormatMessage(
				UInt32 dwFlags,
				IntPtr lpSource,
				Int32 dwMessageId,
				Int32 dwLanguageId,
				StringBuilder lpBuffer,
				Int32 nSize,
				IntPtr Arguments);

		// Windows-specific constants
		private const UInt32 FILE_READ_ATTRIBUTES = 0x80;
		private const UInt32 FILE_WRITE_ATTRIBUTES = 0x0100;
		private const UInt32 FILE_SHARE_READ = 0x1;
		private const UInt32 FILE_SHARE_WRITE = 0x2;
		private const UInt32 OPEN_EXISTING = 3;
		private const UInt32 FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
		private const Int32 FILE_NAME_NORMALIZED = 0x0;
		private const UInt32 FORMAT_MESSAGE_FROM_SYSTEM = 0x00001000;
		private const UInt32 FORMAT_MESSAGE_IGNORE_INSERTS = 0x00000200;

		/// <summary>
		/// Gets a readable description for a Win32 error code
		/// </summary>
		private static String GetWin32ErrorMessage(Int32 errorCode) {
			try {
				StringBuilder message = new StringBuilder(1024);
				Int32 size = FormatMessage(
						FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
						IntPtr.Zero,
						errorCode,
						0, // Default language
						message,
						message.Capacity,
						IntPtr.Zero);

				if (size > 0) {
					return message.ToString().Trim();
				}
			}
			catch (Exception) {
				// Fall back to just the error code if we can't get the message
			}

			return $"Unknown error {errorCode}";
		}

		/// <summary>
		/// Gets the final path name for a file, resolving any symbolic links
		/// </summary>
		/// <param name="path">The path to resolve</param>
		/// <param name="readOnly">If true, opens file with read-only access; if false, opens with read-write access</param>
		/// <returns>The resolved final path, or null if resolution fails</returns>
		public static String? GetFinalPathName(String path, Boolean readOnly = true) {
			Logger.LogDebug($"Resolving path with Windows API (readOnly={readOnly}): {path}");

			IntPtr handle = IntPtr.Zero;

			try {
				// Set access and share mode based on readOnly parameter
				UInt32 desiredAccess = readOnly
						? FILE_READ_ATTRIBUTES
						: FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES;

				UInt32 shareMode = readOnly
						? FILE_SHARE_READ | FILE_SHARE_WRITE
						: FILE_SHARE_READ;

				// Open a handle to the file or directory
				handle = CreateFile(
						path,
						desiredAccess,
						shareMode,
						IntPtr.Zero,
						OPEN_EXISTING,
						FILE_FLAG_BACKUP_SEMANTICS, // Required for directories
						IntPtr.Zero);

				if (handle == new IntPtr(-1)) {
					Int32 error = Marshal.GetLastWin32Error();
					String errorMessage = GetWin32ErrorMessage(error);

					Logger.LogWarning($"CreateFile failed for '{path}' (readOnly={readOnly}). Error {error}: {errorMessage}");

					// If failed with read-write access, retry with read-only access
					if (!readOnly) {
						Logger.LogDebug($"Retrying '{path}' with read-only access");
						return GetFinalPathName(path, true);
					}

					return null;
				}

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
				String finalErrorMessage = GetWin32ErrorMessage(finalError);
				Logger.LogWarning($"GetFinalPathNameByHandle failed for '{path}'. Error {finalError}: {finalErrorMessage}");
				return null;
			}
			catch (Exception ex) {
				Logger.LogWarning($"Exception resolving path '{path}': {ex.GetType().Name}: {ex.Message}");
				return null;
			}
			finally {
				// Always close the handle if it was opened successfully
				if (handle != IntPtr.Zero && handle != new IntPtr(-1)) {
					CloseHandle(handle);
				}
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

			_maxConcurrentOperations = Math.Max(16, Environment.ProcessorCount * 4); // Increase max limit
			_ioThresholdMBPerSec = ioThresholdMBPerSec;

			// Ensure initialConcurrentOperations doesn't exceed the maximum
			_currentLimit = Math.Min(initialConcurrentOperations, _maxConcurrentOperations);

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