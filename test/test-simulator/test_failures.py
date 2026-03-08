#!/usr/bin/env python3
"""
Test script for the failure system
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.failure_manager import FailureManager
from utils.probabilities_calculator import FailureType, FailureImpact
from entities.simulation_params import SimulationParams
from datetime import datetime, timedelta
import time

def test_failure_system():
    """Test the failure system independently"""
    print("Testing Solar PV Failure System")
    print("=" * 50)
    
    # Create failure manager
    failure_manager = FailureManager(print)
    
    # Create simulation parameters
    params = SimulationParams()
    params.start = datetime.now()
    params.end = datetime.now() + timedelta(minutes=10)
    params.speed = 60  # Fast simulation
    params.failure_system_enabled = True
    params.inverter_failure_prob = 10.0  # Higher for testing
    params.bad_connection_prob = 15.0
    params.cloud_day_prob = 30.0
    
    failure_manager.set_params(params)
    
    # Register test components
    components = [
        ("inverter_A0", "inverter"),
        ("inverter_A1", "inverter"),
        ("inverter_B0", "inverter"),
        ("meteo_station", "meteo_station"),
        ("poi_meter", "poi_meter")
    ]
    
    for comp_id, comp_type in components:
        failure_manager.register_component(
            comp_id, comp_type,
            lambda f: print(f"  -> Component failure callback: {f['component_id']} - {f['type'].value}"),
            lambda f: print(f"  -> Component recovery callback: {f['component_id']} - {f['type'].value}")
        )
    
    # Add system status callback
    def system_status_callback(status):
        impact = status['system_impact']
        if impact['total_failures'] > 0:
            print(f"System Status: {impact['total_failures']} failures, "
                  f"stress: {impact['stress_level']:.2f}, "
                  f"performance: {100-impact['performance_reduction']*100:.1f}%")
    
    failure_manager.add_system_status_callback(system_status_callback)
    
    print("Registered components:")
    for comp_id, comp_type in components:
        print(f"  - {comp_id} ({comp_type})")
    print()
    
    # Test forced failures
    print("Testing forced failures...")
    failure_manager.force_failure("inverter_A0", FailureType.INVERTER_OVERHEATING, 30)
    failure_manager.force_failure("meteo_station", FailureType.SENSOR_MALFUNCTION, 60)
    
    time.sleep(2)
    
    # Update environmental conditions
    failure_manager.update_environmental_conditions(35.0, 80.0, 15.0, 900.0)
    
    # Simulate failure evaluation over time
    print("\nRunning failure evaluation simulation for 2 minutes...")
    start_time = datetime.now()
    current_time = start_time
    end_time = start_time + timedelta(minutes=2)
    
    step_count = 0
    while current_time < end_time:
        # Evaluate failures every 10 seconds
        failure_manager.evaluate_failures(current_time, 0.003)  # ~10 seconds in hours
        
        step_count += 1
        if step_count % 6 == 0:  # Every minute
            print(f"\nStep {step_count} - {current_time.strftime('%H:%M:%S')}")
            system_status = failure_manager.get_system_status()
            print(f"  Active failures: {system_status['system_impact']['total_failures']}")
            print(f"  Failed components: {system_status['failed_components']}")
            
            # Show component performance
            for comp_id, _ in components[:3]:  # Show first 3 components
                perf = failure_manager.get_component_performance_multiplier(comp_id)
                status = failure_manager.get_component_status(comp_id)
                print(f"  {comp_id}: {perf:.2f} performance, {len(status['active_failures'])} failures")
        
        current_time += timedelta(seconds=10)
        time.sleep(0.5)  # Real time delay
    
    print("\nFinal system status:")
    final_status = failure_manager.get_system_status()
    print(f"Total components: {final_status['total_components']}")
    print(f"Healthy components: {final_status['healthy_components']}")
    print(f"Failed components: {final_status['failed_components']}")
    print(f"System stress: {final_status['system_impact']['stress_level']:.2f}")
    print(f"Performance reduction: {final_status['system_impact']['performance_reduction']*100:.1f}%")
    
    # Clear all failures
    print("\nClearing all failures...")
    failure_manager.clear_all_failures()
    
    final_status = failure_manager.get_system_status()
    print(f"After clearing - Healthy: {final_status['healthy_components']}, Failed: {final_status['failed_components']}")
    
    print("\nFailure system test completed!")

if __name__ == "__main__":
    test_failure_system()
