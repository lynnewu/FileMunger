
using System.Diagnostics;

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

