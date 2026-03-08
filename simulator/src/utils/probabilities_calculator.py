import random
import math
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from enum import Enum

class FailureType(Enum):
    """Types of failures that can occur in the system"""
    INVERTER_HARDWARE_FAILURE = "inverter_hardware_failure"
    INVERTER_OVERHEATING = "inverter_overheating"
    CONNECTION_FAILURE = "connection_failure"
    WEATHER_INTERFERENCE = "weather_interference"
    GRID_DISTURBANCE = "grid_disturbance"
    SENSOR_MALFUNCTION = "sensor_malfunction"
    POWER_QUALITY_ISSUE = "power_quality_issue"
    MAINTENANCE_SHUTDOWN = "maintenance_shutdown"
    STRING_FAILURE = "string_failure"

class FailureImpact(Enum):
    """Impact levels of failures"""
    LOW = "low"           # Minor performance degradation
    MEDIUM = "medium"     # Moderate impact on efficiency
    HIGH = "high"         # Significant impact or component shutdown
    CRITICAL = "critical" # System-wide impact

class SystemState:
    """Represents the current state of the entire system"""
    def __init__(self):
        self.active_failures: Dict[str, Dict] = {}  # component_id -> failure_info
        self.cascade_multiplier = 1.0  # Multiplier for cascading failures
        self.system_stress_level = 0.0  # 0-1, overall system stress
        self.environmental_factors = {
            'temperature': 25.0,
            'humidity': 50.0,
            'wind_speed': 5.0,
            'irradiance': 800.0
        }

class ProbabilitiesCalculator:
    """
    Advanced probability calculator for system failures and their interconnected effects.
    Maintains relationships between different components and failure types.
    """
    
    def __init__(self):
        self.system_state = SystemState()
        
        # Base failure probabilities per hour (adjustable via params)
        self.base_probabilities = {
            FailureType.INVERTER_HARDWARE_FAILURE: 0.0001,  # 0.01% per hour
            FailureType.INVERTER_OVERHEATING: 0.0005,       # 0.05% per hour
            FailureType.CONNECTION_FAILURE: 0.001,          # 0.1% per hour
            FailureType.WEATHER_INTERFERENCE: 0.002,        # 0.2% per hour
            FailureType.GRID_DISTURBANCE: 0.0003,           # 0.03% per hour
            FailureType.SENSOR_MALFUNCTION: 0.0002,         # 0.02% per hour
            FailureType.POWER_QUALITY_ISSUE: 0.0008,        # 0.08% per hour
            FailureType.MAINTENANCE_SHUTDOWN: 0.00001,      # 0.001% per hour
            FailureType.STRING_FAILURE: 0.0005             # 0.05% per hour (rare string event)
        }
        
        # Failure duration ranges (min, max in minutes)
        self.failure_durations = {
            FailureType.INVERTER_HARDWARE_FAILURE: (60, 480),    # 1-8 hours
            FailureType.INVERTER_OVERHEATING: (5, 60),           # 5-60 minutes
            FailureType.CONNECTION_FAILURE: (1, 30),             # 1-30 minutes
            FailureType.WEATHER_INTERFERENCE: (30, 180),         # 30 minutes - 3 hours
            FailureType.GRID_DISTURBANCE: (1, 15),               # 1-15 minutes
            FailureType.SENSOR_MALFUNCTION: (10, 120),           # 10 minutes - 2 hours
            FailureType.POWER_QUALITY_ISSUE: (5, 45),            # 5-45 minutes
            FailureType.MAINTENANCE_SHUTDOWN: (120, 1440),       # 2-24 hours
            FailureType.STRING_FAILURE: (30, 240)                # 0.5-4 hours
        }
        
        # Cascade relationships: failure_type -> [(affected_failure_type, multiplier)]
        self.cascade_relationships = {
            FailureType.INVERTER_OVERHEATING: [
                (FailureType.INVERTER_HARDWARE_FAILURE, 3.0),
                (FailureType.POWER_QUALITY_ISSUE, 2.0)
            ],
            FailureType.GRID_DISTURBANCE: [
                (FailureType.INVERTER_HARDWARE_FAILURE, 2.5),
                (FailureType.POWER_QUALITY_ISSUE, 4.0),
                (FailureType.CONNECTION_FAILURE, 1.5)
            ],
            FailureType.WEATHER_INTERFERENCE: [
                (FailureType.CONNECTION_FAILURE, 2.0),
                (FailureType.SENSOR_MALFUNCTION, 1.8)
            ],
            FailureType.POWER_QUALITY_ISSUE: [
                (FailureType.INVERTER_HARDWARE_FAILURE, 1.5),
                (FailureType.SENSOR_MALFUNCTION, 1.3)
            ]
        }
        
        # Environmental impact factors
        self.environmental_multipliers = {
            FailureType.INVERTER_OVERHEATING: {
                'temperature': lambda t: max(1.0, (t - 25) * 0.1 + 1.0),  # Increases with temperature
                'irradiance': lambda i: max(1.0, i / 1000 * 0.5 + 0.5)    # Increases with irradiance
            },
            FailureType.CONNECTION_FAILURE: {
                'humidity': lambda h: max(1.0, h / 100 * 2.0 + 0.5),      # Increases with humidity
                'wind_speed': lambda w: max(1.0, w / 20 * 1.5 + 0.5)      # Increases with wind
            },
            FailureType.WEATHER_INTERFERENCE: {
                'humidity': lambda h: max(1.0, h / 100 * 3.0),            # Strong humidity impact
                'wind_speed': lambda w: max(1.0, w / 15 * 2.0)             # Strong wind impact
            }
        }
    
    def update_system_params(self, params):
        """Update system parameters from simulation params"""
        if hasattr(params, 'inverter_failure_prob') and params.inverter_failure_prob:
            # Scale base probabilities based on user input
            scale_factor = params.inverter_failure_prob / 100.0 / 0.001  # Normalize to base
            self.base_probabilities[FailureType.INVERTER_HARDWARE_FAILURE] *= scale_factor
            self.base_probabilities[FailureType.INVERTER_OVERHEATING] *= scale_factor
            
        if hasattr(params, 'bad_connection_prob') and params.bad_connection_prob:
            scale_factor = params.bad_connection_prob / 100.0 / 0.01
            self.base_probabilities[FailureType.CONNECTION_FAILURE] *= scale_factor
            
        if hasattr(params, 'cloud_day_prob') and params.cloud_day_prob:
            scale_factor = params.cloud_day_prob / 100.0 / 0.02
            self.base_probabilities[FailureType.WEATHER_INTERFERENCE] *= scale_factor
    
    def update_environmental_conditions(self, temperature: float, humidity: float, 
                                      wind_speed: float, irradiance: float):
        """Update environmental conditions that affect failure probabilities"""
        self.system_state.environmental_factors.update({
            'temperature': temperature,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'irradiance': irradiance
        })
    
    def calculate_failure_probability(self, failure_type: FailureType, component_id: str, 
                                    time_step_hours: float = 1.0) -> float:
        """
        Calculate the probability of a specific failure type for a component.
        
        Args:
            failure_type: Type of failure to calculate probability for
            component_id: ID of the component
            time_step_hours: Time step in hours for probability calculation
            
        Returns:
            Probability of failure (0-1)
        """
        base_prob = self.base_probabilities.get(failure_type, 0.0)
        
        # Apply time scaling
        scaled_prob = 1 - math.pow(1 - base_prob, time_step_hours)
        
        # Apply environmental multipliers
        env_multiplier = 1.0
        if failure_type in self.environmental_multipliers:
            for factor_name, multiplier_func in self.environmental_multipliers[failure_type].items():
                factor_value = self.system_state.environmental_factors.get(factor_name, 0)
                env_multiplier *= multiplier_func(factor_value)
        
        # Apply cascade effects from existing failures
        cascade_multiplier = self._calculate_cascade_multiplier(failure_type, component_id)
        
        # Apply system stress
        stress_multiplier = 1.0 + self.system_state.system_stress_level * 0.5
        
        final_prob = scaled_prob * env_multiplier * cascade_multiplier * stress_multiplier
        
        return min(final_prob, 0.5)  # Cap at 50% per time step
    
    def _calculate_cascade_multiplier(self, failure_type: FailureType, component_id: str) -> float:
        """Calculate cascade effect multiplier based on existing failures"""
        multiplier = 1.0
        
        # Check for failures that cascade to this failure type
        for existing_failure in self.system_state.active_failures.values():
            existing_type = existing_failure.get('type')
            if existing_type in self.cascade_relationships:
                for cascade_type, cascade_mult in self.cascade_relationships[existing_type]:
                    if cascade_type == failure_type:
                        # Apply distance-based reduction for non-same component
                        if existing_failure.get('component_id') != component_id:
                            distance_reduction = self._calculate_component_distance_factor(
                                existing_failure.get('component_id'), component_id)
                            multiplier *= (cascade_mult * distance_reduction)
                        else:
                            multiplier *= cascade_mult
        
        return multiplier
    
    def _calculate_component_distance_factor(self, comp1: str, comp2: str) -> float:
        """
        Calculate distance factor between components for cascade effects.
        Components in the same group have stronger relationships.
        """
        if not comp1 or not comp2:
            return 0.5
            
        # Same component
        if comp1 == comp2:
            return 1.0
            
        # Extract component types and groups
        comp1_parts = comp1.split('_') if '_' in comp1 else [comp1]
        comp2_parts = comp2.split('_') if '_' in comp2 else [comp2]
        
        # Same type (e.g., both inverters)
        if comp1_parts[0] == comp2_parts[0]:
            # Same group (e.g., both in group A)
            if len(comp1_parts) > 1 and len(comp2_parts) > 1 and comp1_parts[1][0] == comp2_parts[1][0]:
                return 0.8
            else:
                return 0.6
        
        # Different types but related (inverter <-> meteo station)
        related_pairs = [
            ('inverter', 'meteo'),
            ('inverter', 'poi_meter'),
            ('meteo', 'poi_meter')
        ]
        
        for type1, type2 in related_pairs:
            if ((type1 in comp1_parts[0] and type2 in comp2_parts[0]) or
                (type2 in comp1_parts[0] and type1 in comp2_parts[0])):
                return 0.4
        
        return 0.2  # Weak relationship
    
    def should_failure_occur(self, failure_type: FailureType, component_id: str, 
                           time_step_hours: float = 1.0) -> bool:
        """
        Determine if a failure should occur based on calculated probability.
        
        Returns:
            True if failure should occur, False otherwise
        """
        probability = self.calculate_failure_probability(failure_type, component_id, time_step_hours)
        return random.random() < probability
    
    def register_failure(self, failure_type: FailureType, component_id: str, 
                        current_time: datetime, severity: FailureImpact = FailureImpact.MEDIUM) -> Dict:
        """
        Register a new failure in the system.
        
        Returns:
            Dictionary containing failure information
        """
        duration_range = self.failure_durations.get(failure_type, (5, 30))
        duration_minutes = random.uniform(duration_range[0], duration_range[1])
        
        failure_info = {
            'type': failure_type,
            'component_id': component_id,
            'start_time': current_time,
            'end_time': current_time + timedelta(minutes=duration_minutes),
            'severity': severity,
            'duration_minutes': duration_minutes,
            'id': f"{component_id}_{failure_type.value}_{current_time.timestamp()}"
        }
        
        self.system_state.active_failures[failure_info['id']] = failure_info
        self._update_system_stress()
        
        return failure_info
    
    def resolve_expired_failures(self, current_time: datetime) -> List[Dict]:
        """
        Remove failures that have expired and return list of resolved failures.
        """
        resolved_failures = []
        expired_ids = []
        
        for failure_id, failure_info in self.system_state.active_failures.items():
            if current_time >= failure_info['end_time']:
                expired_ids.append(failure_id)
                resolved_failures.append(failure_info)
        
        for failure_id in expired_ids:
            del self.system_state.active_failures[failure_id]
        
        self._update_system_stress()
        return resolved_failures
    
    def get_active_failures_for_component(self, component_id: str) -> List[Dict]:
        """Get all active failures affecting a specific component"""
        return [failure for failure in self.system_state.active_failures.values() 
                if failure['component_id'] == component_id]
    
    def get_system_wide_impact(self) -> Dict:
        """Calculate system-wide impact of all active failures"""
        impact = {
            'total_failures': len(self.system_state.active_failures),
            'stress_level': self.system_state.system_stress_level,
            'performance_reduction': 0.0,
            'affected_components': set(),
            'critical_failures': 0
        }
        
        for failure in self.system_state.active_failures.values():
            impact['affected_components'].add(failure['component_id'])
            if failure['severity'] == FailureImpact.CRITICAL:
                impact['critical_failures'] += 1
                impact['performance_reduction'] += 0.3
            elif failure['severity'] == FailureImpact.HIGH:
                impact['performance_reduction'] += 0.2
            elif failure['severity'] == FailureImpact.MEDIUM:
                impact['performance_reduction'] += 0.1
            else:  # LOW
                impact['performance_reduction'] += 0.05
        
        impact['performance_reduction'] = min(impact['performance_reduction'], 0.95)  # Cap at 95%
        impact['affected_components'] = list(impact['affected_components'])
        
        return impact
    
    def _update_system_stress(self):
        """Update overall system stress level based on active failures"""
        if not self.system_state.active_failures:
            self.system_state.system_stress_level = 0.0
            return
        
        stress = 0.0
        for failure in self.system_state.active_failures.values():
            if failure['severity'] == FailureImpact.CRITICAL:
                stress += 0.4
            elif failure['severity'] == FailureImpact.HIGH:
                stress += 0.25
            elif failure['severity'] == FailureImpact.MEDIUM:
                stress += 0.15
            else:  # LOW
                stress += 0.05
        
        self.system_state.system_stress_level = min(stress, 1.0)
    
    def calculate_performance_multiplier(self, component_id: str, failure_types: List[FailureType] = None) -> float:
        """
        Calculate performance multiplier for a component based on active failures.
        
        Args:
            component_id: ID of the component
            failure_types: Optional list of specific failure types to consider
            
        Returns:
            Performance multiplier (0.0 = complete failure, 1.0 = no impact)
        """
        multiplier = 1.0
        component_failures = self.get_active_failures_for_component(component_id)
        
        for failure in component_failures:
            if failure_types is None or failure['type'] in failure_types:
                if failure['severity'] == FailureImpact.CRITICAL:
                    multiplier *= 0.0  # Complete failure
                elif failure['severity'] == FailureImpact.HIGH:
                    multiplier *= 0.3  # 70% reduction
                elif failure['severity'] == FailureImpact.MEDIUM:
                    multiplier *= 0.6  # 40% reduction
                else:  # LOW
                    multiplier *= 0.9  # 10% reduction
        
        return max(multiplier, 0.0)