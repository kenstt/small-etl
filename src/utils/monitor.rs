#[cfg(feature = "cli")]
use sysinfo::{Pid, RefreshKind, System};
#[cfg(feature = "cli")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "cli")]
use std::time::{Duration, Instant};

#[cfg(feature = "cli")]
#[derive(Debug, Clone)]
pub struct SystemStats {
    pub cpu_usage: f32,
    pub memory_usage_mb: u64,
    pub memory_usage_percent: f32,
    pub peak_memory_mb: u64,
    pub elapsed_time: Duration,
}

#[cfg(feature = "cli")]
pub struct SystemMonitor {
    system: Arc<Mutex<System>>,
    pid: Pid,
    start_time: Instant,
    peak_memory: Arc<Mutex<u64>>,
    enabled: bool,
}

#[cfg(feature = "cli")]
impl SystemMonitor {
    pub fn new(enabled: bool) -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::everything()
        );

        let pid = sysinfo::get_current_pid().expect("Failed to get current PID");

        // 初始刷新
        system.refresh_all();

        Self {
            system: Arc::new(Mutex::new(system)),
            pid,
            start_time: Instant::now(),
            peak_memory: Arc::new(Mutex::new(0)),
            enabled,
        }
    }

    pub fn get_stats(&self) -> Option<SystemStats> {
        if !self.enabled {
            return None;
        }

        let mut system = self.system.lock().ok()?;
        system.refresh_all();

        let process = system.process(self.pid)?;
        let memory_mb = process.memory() / 1024 / 1024; // Convert bytes to MB
        let total_memory = system.total_memory() / 1024 / 1024; // Convert to MB
        let memory_percent = if total_memory > 0 {
            (memory_mb as f32 / total_memory as f32) * 100.0
        } else {
            0.0
        };

        // 更新峰值記憶體
        let mut peak = self.peak_memory.lock().ok()?;
        if memory_mb > *peak {
            *peak = memory_mb;
        }
        let peak_memory = *peak;

        Some(SystemStats {
            cpu_usage: process.cpu_usage(),
            memory_usage_mb: memory_mb,
            memory_usage_percent: memory_percent,
            peak_memory_mb: peak_memory,
            elapsed_time: self.start_time.elapsed(),
        })
    }

    pub fn log_stats(&self, phase: &str) {
        if let Some(stats) = self.get_stats() {
            tracing::info!(
                "📊 {} - CPU: {:.1}%, Memory: {}MB ({:.1}%), Peak: {}MB, Time: {:?}",
                phase,
                stats.cpu_usage,
                stats.memory_usage_mb,
                stats.memory_usage_percent,
                stats.peak_memory_mb,
                stats.elapsed_time
            );
        }
    }

    pub fn log_final_stats(&self) {
        if let Some(stats) = self.get_stats() {
            tracing::info!("📊 Final Stats - Total Time: {:?}, Peak Memory: {}MB",
                stats.elapsed_time, stats.peak_memory_mb);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(feature = "cli")]
impl Default for SystemMonitor {
    fn default() -> Self {
        Self::new(false)
    }
}

// 為非CLI環境提供空實現
#[cfg(not(feature = "cli"))]
pub struct SystemMonitor;

#[cfg(not(feature = "cli"))]
impl SystemMonitor {
    pub fn new(_enabled: bool) -> Self {
        Self
    }

    pub fn log_stats(&self, _phase: &str) {}

    pub fn log_final_stats(&self) {}

    pub fn is_enabled(&self) -> bool {
        false
    }
}