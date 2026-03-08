from utils.probabilities_calculator import ProbabilitiesCalculator, FailureType, FailureImpact
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Optional
import threading
import time

class FailureManager:
    """
    Central failure manager that coordinates failures across all system components.
    Maintains relationships and cascading effects between different components.
    """
    
    def __init__(self, print_callback: Callable = None):
        self.prob_calculator = ProbabilitiesCalculator()
        self.print_callback = print_callback or (lambda x: print(x))
        self.lock = threading.Lock()
        
        # Component registry: component_id -> component_info
        self.components = {}
        
        # Failure event callbacks: component_id -> callback_function
        self.failure_callbacks = {}
        
        # Recovery event callbacks: component_id -> callback_function  
        self.recovery_callbacks = {}
        
        # System-wide status callbacks
        self.system_status_callbacks = []
        
        self.running = False
        self.params = None
        
    def register_component(self, component_id: str, component_type: str, 
                          failure_callback: Callable = None, 
                          recovery_callback: Callable = None):
        """
        Register a component with the failure manager.
        
        Args:
            component_id: Unique identifier for the component
            component_type: Type of component (inverter, meteo_station, etc.)
            failure_callback: Function to call when failure occurs
            recovery_callback: Function to call when component recovers
        """
        with self.lock:
            self.components[component_id] = {
                'type': component_type,
                'registered_at': datetime.now(),
                'last_health_check': datetime.now(),
                'status': 'healthy'
            }
            
            if failure_callback:
                self.failure_callbacks[component_id] = failure_callback
            if recovery_callback:
                self.recovery_callbacks[component_id] = recovery_callback
                
            self.print_callback(f"Failure Manager: Registered component {component_id} ({component_type})")
    
    def unregister_component(self, component_id: str):
        """Unregister a component from the failure manager"""
        with self.lock:
            if component_id in self.components:
                del self.components[component_id]
            if component_id in self.failure_callbacks:
                del self.failure_callbacks[component_id]
            if component_id in self.recovery_callbacks:
                del self.recovery_callbacks[component_id]
                
    def set_params(self, params):
        """Set simulation parameters"""
        self.params = params
        self.prob_calculator.update_system_params(params)
        
    def add_system_status_callback(self, callback: Callable):
        """Add a callback for system-wide status updates"""
        self.system_status_callbacks.append(callback)
        
    def update_environmental_conditions(self, temperature: float, humidity: float,
                                      wind_speed: float, irradiance: float):
        """Update environmental conditions that affect failure probabilities"""
        self.prob_calculator.update_environmental_conditions(
            temperature, humidity, wind_speed, irradiance)
            
    def evaluate_failures(self, current_time: datetime, time_step_hours: float = 1.0):
        """
        Evaluate potential failures for all registered components.
        This should be called periodically during simulation.
        """
        if not self.params or not getattr(self.params, 'failure_system_enabled', True):
            return
            
        with self.lock:
            # First, resolve any expired failures
            resolved_failures = self.prob_calculator.resolve_expired_failures(current_time)
            for failure in resolved_failures:
                self._handle_failure_recovery(failure, current_time)
            
            # Then check for new failures
            for component_id, component_info in self.components.items():
                component_type = component_info['type']
                
                # Determine relevant failure types for this component
                relevant_failures = self._get_relevant_failure_types(component_type)
                
                for failure_type in relevant_failures:
                    if self.prob_calculator.should_failure_occur(
                        failure_type, component_id, time_step_hours):
                        
                        # Determine failure severity based on type and current system state
                        severity = self._determine_failure_severity(failure_type, component_id)
                        
                        # Register the failure
                        failure_info = self.prob_calculator.register_failure(
                            failure_type, component_id, current_time, severity)
                        
                        # Handle the failure
                        self._handle_new_failure(failure_info, current_time)
            
            # Update system status
            self._update_system_status(current_time)
    
    def _get_relevant_failure_types(self, component_type: str) -> List[FailureType]:
        """Get failure types relevant to a specific component type"""
        failure_mapping = {
            'inverter': [
                FailureType.INVERTER_HARDWARE_FAILURE,
                FailureType.INVERTER_OVERHEATING,
                FailureType.POWER_QUALITY_ISSUE,
                FailureType.GRID_DISTURBANCE,
                FailureType.CONNECTION_FAILURE,
                FailureType.MAINTENANCE_SHUTDOWN,
                FailureType.STRING_FAILURE
            ],
            # 'meteo_station' intentionally omitted: no failures for meteo station
            'poi_meter': [
                FailureType.CONNECTION_FAILURE,
                FailureType.SENSOR_MALFUNCTION,
                FailureType.GRID_DISTURBANCE,
                FailureType.MAINTENANCE_SHUTDOWN
            ],
            'grid_interface': [
                FailureType.GRID_DISTURBANCE,
                FailureType.POWER_QUALITY_ISSUE,
                FailureType.CONNECTION_FAILURE,
                FailureType.MAINTENANCE_SHUTDOWN
            ]
        }
        # Meteo station will not have failures
        if component_type == 'meteo_station':
            return []
        # Default to basic failures if component type not recognized
        return failure_mapping.get(component_type, [
            FailureType.CONNECTION_FAILURE,
            FailureType.MAINTENANCE_SHUTDOWN
        ])
    
    def _determine_failure_severity(self, failure_type: FailureType, component_id: str) -> FailureImpact:
        """Determine the severity of a failure based on context"""
        system_impact = self.prob_calculator.get_system_wide_impact()
        
        # Base severity mapping
        severity_mapping = {
            FailureType.INVERTER_HARDWARE_FAILURE: FailureImpact.HIGH,
            FailureType.INVERTER_OVERHEATING: FailureImpact.MEDIUM,
            FailureType.CONNECTION_FAILURE: FailureImpact.LOW,
            FailureType.WEATHER_INTERFERENCE: FailureImpact.MEDIUM,
            FailureType.GRID_DISTURBANCE: FailureImpact.HIGH,
            FailureType.SENSOR_MALFUNCTION: FailureImpact.LOW,
            FailureType.POWER_QUALITY_ISSUE: FailureImpact.MEDIUM,
            FailureType.MAINTENANCE_SHUTDOWN: FailureImpact.HIGH
        }
        
        base_severity = severity_mapping.get(failure_type, FailureImpact.MEDIUM)
        
        # Escalate severity if system is already under stress
        if system_impact['stress_level'] > 0.6:
            if base_severity == FailureImpact.LOW:
                return FailureImpact.MEDIUM
            elif base_severity == FailureImpact.MEDIUM:
                return FailureImpact.HIGH
            elif base_severity == FailureImpact.HIGH:
                return FailureImpact.CRITICAL
                
        return base_severity
    
    def _handle_new_failure(self, failure_info: Dict, current_time: datetime):
        """Handle a new failure occurrence"""
        component_id = failure_info['component_id']
        failure_type = failure_info['type']
        severity = failure_info['severity']
        
        # Update component status
        if component_id in self.components:
            self.components[component_id]['status'] = f'failed_{severity.value}'
            
        # Log the failure
        duration_str = f"{failure_info['duration_minutes']:.1f} minutes"
        self.print_callback(
            f"FAILURE: {component_id} - {failure_type.value.replace('_', ' ').title()} "
            f"({severity.value.upper()}) - Duration: {duration_str}"
        )
        
        # Call component-specific failure callback
        if component_id in self.failure_callbacks:
            try:
                self.failure_callbacks[component_id](failure_info)
            except Exception as e:
                self.print_callback(f"Error in failure callback for {component_id}: {e}")
    
    def _handle_failure_recovery(self, failure_info: Dict, current_time: datetime):
        """Handle recovery from a failure"""
        component_id = failure_info['component_id']
        failure_type = failure_info['type']
        
        # Update component status if no other failures remain
        remaining_failures = self.prob_calculator.get_active_failures_for_component(component_id)
        if not remaining_failures:
            if component_id in self.components:
                self.components[component_id]['status'] = 'healthy'
        
        # Log the recovery
        self.print_callback(
            f"RECOVERY: {component_id} - Recovered from {failure_type.value.replace('_', ' ').title()}"
        )
        
        # Call component-specific recovery callback
        if component_id in self.recovery_callbacks:
            try:
                self.recovery_callbacks[component_id](failure_info)
            except Exception as e:
                self.print_callback(f"Error in recovery callback for {component_id}: {e}")
    
    def _update_system_status(self, current_time: datetime):
        """Update and broadcast system-wide status"""
        system_impact = self.prob_calculator.get_system_wide_impact()
        
        # Check if system stress exceeds threshold
        stress_threshold = getattr(self.params, 'system_stress_threshold', 0.7)
        if system_impact['stress_level'] > stress_threshold:
            self.print_callback(
                f"WARNING: System stress level high ({system_impact['stress_level']:.2f}) - "
                f"{system_impact['total_failures']} active failures"
            )
        
        # Broadcast to system status callbacks
        status_info = {
            'timestamp': current_time,
            'system_impact': system_impact,
            'component_count': len(self.components),
            'healthy_components': len([c for c in self.components.values() 
                                     if c['status'] == 'healthy'])
        }
        
        for callback in self.system_status_callbacks:
            try:
                callback(status_info)
            except Exception as e:
                self.print_callback(f"Error in system status callback: {e}")
    
    def get_component_performance_multiplier(self, component_id: str) -> float:
        """Get performance multiplier for a component based on active failures"""
        return self.prob_calculator.calculate_performance_multiplier(component_id)
    
    def get_component_status(self, component_id: str) -> Dict:
        """Get detailed status information for a component"""
        if component_id not in self.components:
            return None
            
        component_info = self.components[component_id].copy()
        active_failures = self.prob_calculator.get_active_failures_for_component(component_id)
        performance_multiplier = self.get_component_performance_multiplier(component_id)
        
        return {
            **component_info,
            'active_failures': active_failures,
            'performance_multiplier': performance_multiplier,
            'failure_count': len(active_failures)
        }
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        system_impact = self.prob_calculator.get_system_wide_impact()
        
        return {
            'system_impact': system_impact,
            'total_components': len(self.components),
            'healthy_components': len([c for c in self.components.values() 
                                     if c['status'] == 'healthy']),
            'failed_components': len([c for c in self.components.values() 
                                    if c['status'] != 'healthy']),
            'components': {cid: self.get_component_status(cid) 
                          for cid in self.components.keys()}
        }
    
    def force_failure(self, component_id: str, failure_type: FailureType, 
                     duration_minutes: float = None, severity: FailureImpact = None):
        """Force a specific failure for testing purposes"""
        if component_id not in self.components:
            self.print_callback(f"Cannot force failure: Component {component_id} not registered")
            return None
            
        current_time = datetime.now()
        
        if duration_minutes is None:
            duration_range = self.prob_calculator.failure_durations.get(failure_type, (5, 30))
            duration_minutes = (duration_range[0] + duration_range[1]) / 2
            
        if severity is None:
            severity = self._determine_failure_severity(failure_type, component_id)
        
        # Create failure manually
        failure_info = {
            'type': failure_type,
            'component_id': component_id,
            'start_time': current_time,
            'end_time': current_time + timedelta(minutes=duration_minutes),
            'severity': severity,
            'duration_minutes': duration_minutes,
            'id': f"{component_id}_{failure_type.value}_{current_time.timestamp()}_forced"
        }
        
        with self.lock:
            self.prob_calculator.system_state.active_failures[failure_info['id']] = failure_info
            self.prob_calculator._update_system_stress()
            self._handle_new_failure(failure_info, current_time)
        
        return failure_info
    
    def clear_all_failures(self):
        """Clear all active failures (for testing/reset purposes)"""
        with self.lock:
            resolved_failures = list(self.prob_calculator.system_state.active_failures.values())
            self.prob_calculator.system_state.active_failures.clear()
            self.prob_calculator._update_system_stress()
            
            current_time = datetime.now()
            for failure in resolved_failures:
                self._handle_failure_recovery(failure, current_time)
                
            self.print_callback("All failures cleared by system reset")
