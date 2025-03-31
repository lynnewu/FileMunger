
using System.Diagnostics;

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



