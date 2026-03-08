class SimulationParams:
    def __init__(self): 
        
        # Main properties
        self.start = None
        self.end = None
        self.speed = 1
        
        # Failure probabilities and configurations
        self.consume_above_gen_day_prob = None
        self.cloud_day_prob = None
        self.inverter_failure_prob = None
        self.inverter_failure_time_range = None
        self.bad_connection_prob = None
        self.bad_connection_time_range = None
        
        # Advanced failure system configuration
        self.enable_cascade_failures = True      # Enable cascading failure effects
        self.enable_environmental_effects = True # Enable environmental impact on failures
        self.failure_system_enabled = True      # Master switch for failure system
        self.system_stress_threshold = 0.7      # Threshold for high system stress warnings
        
        # Specific failure type probabilities (optional overrides)
        self.inverter_overheating_prob = None
        self.grid_disturbance_prob = None
        self.sensor_malfunction_prob = None
        self.power_quality_issue_prob = None
        self.maintenance_shutdown_prob = None
        self.weather_interference_prob = None