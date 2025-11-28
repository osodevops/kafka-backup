//! Health monitoring for backup/restore processes.
//!
//! Provides health checks and status reporting for monitoring system health.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// All systems operational
    Healthy,
    /// Some issues but still operational
    Degraded,
    /// Critical issues, system may not be functioning
    Unhealthy,
}

impl Default for HealthStatus {
    fn default() -> Self {
        HealthStatus::Healthy
    }
}

/// Individual component health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Current status
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
    /// Last check timestamp (epoch ms)
    pub last_checked: i64,
    /// Time since last successful operation (ms)
    pub last_success_ms: Option<u64>,
}

/// Overall health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    /// Overall status (worst of all components)
    pub status: HealthStatus,
    /// Process uptime in seconds
    pub uptime_secs: f64,
    /// Individual component health
    pub components: Vec<ComponentHealth>,
    /// Active backup/restore jobs
    pub active_jobs: usize,
    /// Total records processed
    pub records_processed: u64,
    /// Current throughput (records/sec)
    pub current_throughput: f64,
}

/// Health check manager
pub struct HealthCheck {
    start_time: Instant,
    components: RwLock<HashMap<String, ComponentState>>,
    records_processed: AtomicU64,
    active_jobs: AtomicU64,
    recent_throughput: RwLock<Vec<(Instant, u64)>>,
}

struct ComponentState {
    status: HealthStatus,
    message: Option<String>,
    last_checked: Instant,
    last_success: Option<Instant>,
}

impl Default for HealthCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthCheck {
    /// Create a new health check manager
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            components: RwLock::new(HashMap::new()),
            records_processed: AtomicU64::new(0),
            active_jobs: AtomicU64::new(0),
            recent_throughput: RwLock::new(Vec::with_capacity(60)),
        }
    }

    /// Register a component
    pub fn register_component(&self, name: &str) {
        let mut components = self.components.write();
        components.insert(
            name.to_string(),
            ComponentState {
                status: HealthStatus::Healthy,
                message: None,
                last_checked: Instant::now(),
                last_success: Some(Instant::now()),
            },
        );
        debug!("Registered health component: {}", name);
    }

    /// Update component status
    pub fn update_component(&self, name: &str, status: HealthStatus, message: Option<&str>) {
        let mut components = self.components.write();
        let now = Instant::now();

        if let Some(state) = components.get_mut(name) {
            let was_healthy = state.status == HealthStatus::Healthy;
            state.status = status;
            state.message = message.map(|s| s.to_string());
            state.last_checked = now;

            if status == HealthStatus::Healthy {
                state.last_success = Some(now);
            }

            // Log status changes
            if was_healthy && status != HealthStatus::Healthy {
                warn!("Component {} became {:?}: {:?}", name, status, message);
            } else if !was_healthy && status == HealthStatus::Healthy {
                info!("Component {} recovered", name);
            }
        } else {
            components.insert(
                name.to_string(),
                ComponentState {
                    status,
                    message: message.map(|s| s.to_string()),
                    last_checked: now,
                    last_success: if status == HealthStatus::Healthy {
                        Some(now)
                    } else {
                        None
                    },
                },
            );
        }
    }

    /// Mark component as healthy
    pub fn mark_healthy(&self, name: &str) {
        self.update_component(name, HealthStatus::Healthy, None);
    }

    /// Mark component as degraded
    pub fn mark_degraded(&self, name: &str, message: &str) {
        self.update_component(name, HealthStatus::Degraded, Some(message));
    }

    /// Mark component as unhealthy
    pub fn mark_unhealthy(&self, name: &str, message: &str) {
        self.update_component(name, HealthStatus::Unhealthy, Some(message));
    }

    /// Record records processed
    pub fn record_records(&self, count: u64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);

        // Update throughput tracking
        let mut throughput = self.recent_throughput.write();
        let now = Instant::now();
        throughput.push((now, count));

        // Keep last 60 seconds of data
        let cutoff = now - Duration::from_secs(60);
        throughput.retain(|(t, _)| *t > cutoff);
    }

    /// Increment active jobs
    pub fn job_started(&self) {
        self.active_jobs.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active jobs
    pub fn job_completed(&self) {
        self.active_jobs.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current throughput (records/sec)
    pub fn current_throughput(&self) -> f64 {
        let throughput = self.recent_throughput.read();
        if throughput.is_empty() {
            return 0.0;
        }

        let now = Instant::now();
        let window = Duration::from_secs(10);
        let cutoff = now - window;

        let recent_count: u64 = throughput
            .iter()
            .filter(|(t, _)| *t > cutoff)
            .map(|(_, c)| c)
            .sum();

        recent_count as f64 / window.as_secs_f64()
    }

    /// Get overall health status
    pub fn status(&self) -> HealthStatus {
        let components = self.components.read();

        let mut worst = HealthStatus::Healthy;
        for state in components.values() {
            match state.status {
                HealthStatus::Unhealthy => return HealthStatus::Unhealthy,
                HealthStatus::Degraded => worst = HealthStatus::Degraded,
                HealthStatus::Healthy => {}
            }
        }
        worst
    }

    /// Generate a full health report
    pub fn report(&self) -> HealthReport {
        let components = self.components.read();
        let now = Instant::now();

        let component_health: Vec<ComponentHealth> = components
            .iter()
            .map(|(name, state)| ComponentHealth {
                name: name.clone(),
                status: state.status,
                message: state.message.clone(),
                last_checked: chrono::Utc::now().timestamp_millis()
                    - (now - state.last_checked).as_millis() as i64,
                last_success_ms: state.last_success.map(|t| (now - t).as_millis() as u64),
            })
            .collect();

        HealthReport {
            status: self.status(),
            uptime_secs: self.start_time.elapsed().as_secs_f64(),
            components: component_health,
            active_jobs: self.active_jobs.load(Ordering::Relaxed) as usize,
            records_processed: self.records_processed.load(Ordering::Relaxed),
            current_throughput: self.current_throughput(),
        }
    }

    /// Check if the system is healthy
    pub fn is_healthy(&self) -> bool {
        self.status() == HealthStatus::Healthy
    }

    /// Check if the system is operational (healthy or degraded)
    pub fn is_operational(&self) -> bool {
        self.status() != HealthStatus::Unhealthy
    }
}

impl std::fmt::Display for HealthReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Health Report ===")?;
        writeln!(f, "Status: {:?}", self.status)?;
        writeln!(f, "Uptime: {:.0}s", self.uptime_secs)?;
        writeln!(f, "Active Jobs: {}", self.active_jobs)?;
        writeln!(f, "Records Processed: {}", self.records_processed)?;
        writeln!(f, "Current Throughput: {:.0} rec/s", self.current_throughput)?;
        writeln!(f)?;
        writeln!(f, "Components:")?;
        for comp in &self.components {
            write!(f, "  {}: {:?}", comp.name, comp.status)?;
            if let Some(ref msg) = comp.message {
                write!(f, " - {}", msg)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_basic() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.register_component("storage");

        assert_eq!(health.status(), HealthStatus::Healthy);
        assert!(health.is_healthy());
    }

    #[test]
    fn test_health_degraded() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.mark_degraded("kafka", "High latency");

        assert_eq!(health.status(), HealthStatus::Degraded);
        assert!(health.is_operational());
        assert!(!health.is_healthy());
    }

    #[test]
    fn test_health_unhealthy() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.register_component("storage");
        health.mark_unhealthy("kafka", "Connection failed");

        assert_eq!(health.status(), HealthStatus::Unhealthy);
        assert!(!health.is_operational());
    }

    #[test]
    fn test_health_recovery() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.mark_unhealthy("kafka", "Connection failed");
        assert_eq!(health.status(), HealthStatus::Unhealthy);

        health.mark_healthy("kafka");
        assert_eq!(health.status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_throughput_tracking() {
        let health = HealthCheck::new();

        health.record_records(100);
        health.record_records(200);

        // Throughput should be calculated from recent records
        let throughput = health.current_throughput();
        assert!(throughput > 0.0);
    }

    #[test]
    fn test_health_report() {
        let health = HealthCheck::new();

        health.register_component("kafka");
        health.register_component("storage");
        health.record_records(1000);
        health.job_started();

        let report = health.report();
        assert_eq!(report.status, HealthStatus::Healthy);
        assert_eq!(report.components.len(), 2);
        assert_eq!(report.active_jobs, 1);
        assert_eq!(report.records_processed, 1000);
    }
}
