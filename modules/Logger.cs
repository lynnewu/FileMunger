using System.Diagnostics;

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

