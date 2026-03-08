"""
Plant Configuration Loader for SolarX Simulator
Provides centralized access to plant configuration from plant_config.json

The config file is expected to be in one of these locations (in order of priority):
1. Path specified by PLANT_CONFIG_PATH environment variable
2. /app/config/plant_config.json (Docker container path)
3. <project_root>/config/plant_config.json (local development)
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


def get_config_path() -> Path:
    """Get the path to the plant configuration file"""
    # Check for environment variable override first
    if os.getenv("PLANT_CONFIG_PATH"):
        return Path(os.getenv("PLANT_CONFIG_PATH"))

    # Docker container path (mounted volume)
    docker_path = Path("/app/config/plant_config.json")
    if docker_path.exists():
        return docker_path

    # Local development: look for config at project root
    # Navigate up from simulator/config/ to project root, then into config/
    current_dir = Path(__file__).parent  # simulator/config/
    project_root = current_dir.parent.parent  # solarxai/
    local_path = project_root / "config" / "plant_config.json"
    if local_path.exists():
        return local_path

    # Fallback: check parent directories recursively for config/plant_config.json
    search_path = current_dir
    for _ in range(5):  # Limit search depth
        candidate = search_path / "config" / "plant_config.json"
        if candidate.exists():
            return candidate
        search_path = search_path.parent

    raise FileNotFoundError(
        "Could not find plant_config.json. Searched paths:\n"
        f"  - {docker_path}\n"
        f"  - {local_path}\n"
        "Set PLANT_CONFIG_PATH environment variable to specify the location."
    )


def load_plant_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load the plant configuration from JSON file"""
    path = config_path or get_config_path()

    if not path.exists():
        raise FileNotFoundError(f"Plant configuration file not found: {path}")

    with open(path, 'r') as f:
        return json.load(f)


@dataclass
class DCSpecs:
    """DC-side specifications for an inverter"""
    voltage_mpp_min_v: float
    voltage_mpp_max_v: float
    voltage_nominal_v: float
    current_max_a: float
    strings_count: int
    panels_per_string: int
    panel_wp: float

    @property
    def current_per_string_a(self) -> float:
        """Calculate current contribution per string at MPP"""
        return self.current_max_a / self.strings_count

    @property
    def power_per_string_kw(self) -> float:
        """Calculate power contribution per string"""
        return (self.panels_per_string * self.panel_wp) / 1000


@dataclass
class ACSpecs:
    """AC-side specifications for an inverter"""
    voltage_nominal_v: float
    frequency_nominal_hz: float
    power_factor_nominal: float


@dataclass
class EfficiencySpecs:
    """Efficiency specifications for an inverter"""
    dc_ac_nominal: float
    dc_ac_min_acceptable: float
    system_efficiency_range: tuple


@dataclass
class ThermalSpecs:
    """Thermal specifications for an inverter"""
    temp_coefficient_power_pct_per_c: float
    stc_temperature_c: float
    max_operating_temp_c: float
    thermal_sensitivity_range: tuple


@dataclass
class InverterSpecs:
    """Complete inverter specifications"""
    nominal_capacity_kw: float
    panel_surface_m2: float
    dc: DCSpecs
    ac: ACSpecs
    efficiency: EfficiencySpecs
    thermal: ThermalSpecs

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'InverterSpecs':
        """Create InverterSpecs from configuration dictionary"""
        inv_cfg = config.get('inverter_specs', {})
        dc_cfg = inv_cfg.get('dc_side', {})
        ac_cfg = inv_cfg.get('ac_side', {})
        eff_cfg = inv_cfg.get('efficiency', {})
        thermal_cfg = inv_cfg.get('thermal', {})

        return cls(
            nominal_capacity_kw=inv_cfg.get('nominal_capacity_kw', 90.0),
            panel_surface_m2=inv_cfg.get('panel_surface_m2', 100.0),
            dc=DCSpecs(
                voltage_mpp_min_v=dc_cfg.get('voltage_mpp_min_v', 600),
                voltage_mpp_max_v=dc_cfg.get('voltage_mpp_max_v', 1000),
                voltage_nominal_v=dc_cfg.get('voltage_nominal_v', 800),
                current_max_a=dc_cfg.get('current_max_a', 150),
                strings_count=dc_cfg.get('strings_count', 12),
                panels_per_string=dc_cfg.get('panels_per_string', 20),
                panel_wp=dc_cfg.get('panel_wp', 400)
            ),
            ac=ACSpecs(
                voltage_nominal_v=ac_cfg.get('voltage_nominal_v', 400),
                frequency_nominal_hz=ac_cfg.get('frequency_nominal_hz', 50),
                power_factor_nominal=ac_cfg.get('power_factor_nominal', 0.99)
            ),
            efficiency=EfficiencySpecs(
                dc_ac_nominal=eff_cfg.get('dc_ac_nominal', 0.97),
                dc_ac_min_acceptable=eff_cfg.get('dc_ac_min_acceptable', 0.95),
                system_efficiency_range=tuple(eff_cfg.get(
                    'system_efficiency_range', [0.88, 0.92]))
            ),
            thermal=ThermalSpecs(
                temp_coefficient_power_pct_per_c=thermal_cfg.get(
                    'temp_coefficient_power_pct_per_c', -0.4),
                stc_temperature_c=thermal_cfg.get('stc_temperature_c', 25.0),
                max_operating_temp_c=thermal_cfg.get(
                    'max_operating_temp_c', 60.0),
                thermal_sensitivity_range=tuple(thermal_cfg.get(
                    'thermal_sensitivity', [0.008, 0.012]))
            )
        )


@dataclass
class POIMeterSpecs:
    """POI (Point of Interconnection) Meter specifications"""
    meter_id: str
    meter_type: str
    max_export_kw: float
    max_import_kw: float
    grid_voltage_nominal_v: float
    grid_voltage_tolerance_pct: float

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'POIMeterSpecs':
        """Create POIMeterSpecs from configuration dictionary"""
        poi_cfg = config.get('poi_meter_specs', {})
        power_cfg = poi_cfg.get('power_capacity', {})
        voltage_cfg = poi_cfg.get('grid_voltage', {})

        return cls(
            meter_id=poi_cfg.get('meter_id', 'POI_METER_01'),
            meter_type=poi_cfg.get('meter_type', 'bidirectional'),
            max_export_kw=power_cfg.get('max_export_kw', 1300),
            max_import_kw=power_cfg.get('max_import_kw', 500),
            grid_voltage_nominal_v=voltage_cfg.get('nominal_v', 400),
            grid_voltage_tolerance_pct=voltage_cfg.get('tolerance_pct', 10)
        )


@dataclass
class Thresholds:
    """Alert and warning thresholds"""
    # Inverter thresholds
    efficiency_alert_min: float
    efficiency_warning_min: float
    temp_warning_c: float
    temp_critical_c: float
    freq_deviation_warning_hz: float
    freq_deviation_critical_hz: float

    # DC diagnostics thresholds
    string_current_imbalance_pct: float
    voltage_deviation_pct: float
    min_power_for_efficiency_calc_kw: float

    # POI meter thresholds
    poi_voltage_deviation_warning_pct: float
    poi_voltage_deviation_critical_pct: float
    power_factor_warning_min: float
    power_factor_critical_min: float

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'Thresholds':
        """Create Thresholds from configuration dictionary"""
        thresh = config.get('thresholds', {})
        inv_thresh = thresh.get('inverter', {})
        dc_thresh = thresh.get('dc_diagnostics', {})
        poi_thresh = thresh.get('poi_meter', {})

        return cls(
            efficiency_alert_min=inv_thresh.get('efficiency_alert_min', 0.95),
            efficiency_warning_min=inv_thresh.get(
                'efficiency_warning_min', 0.93),
            temp_warning_c=inv_thresh.get('temp_warning_c', 55.0),
            temp_critical_c=inv_thresh.get('temp_critical_c', 65.0),
            freq_deviation_warning_hz=inv_thresh.get(
                'freq_deviation_warning_hz', 0.5),
            freq_deviation_critical_hz=inv_thresh.get(
                'freq_deviation_critical_hz', 1.0),
            string_current_imbalance_pct=dc_thresh.get(
                'string_current_imbalance_pct', 10.0),
            voltage_deviation_pct=dc_thresh.get('voltage_deviation_pct', 5.0),
            min_power_for_efficiency_calc_kw=dc_thresh.get(
                'min_power_for_efficiency_calc_kw', 1.0),
            poi_voltage_deviation_warning_pct=poi_thresh.get(
                'voltage_deviation_warning_pct', 5.0),
            poi_voltage_deviation_critical_pct=poi_thresh.get(
                'voltage_deviation_critical_pct', 10.0),
            power_factor_warning_min=poi_thresh.get(
                'power_factor_warning_min', 0.90),
            power_factor_critical_min=poi_thresh.get(
                'power_factor_critical_min', 0.85)
        )


@dataclass
class PlantConfig:
    """Complete plant configuration"""
    plant_id: str
    plant_name: str
    total_capacity_kwp: float
    total_surface_m2: float
    inverter_groups: List[Dict[str, Any]]
    inverter_specs: InverterSpecs
    poi_meter_specs: POIMeterSpecs
    thresholds: Thresholds
    raw_config: Dict[str, Any] = field(repr=False)

    @property
    def total_inverters(self) -> int:
        """Get total number of inverters"""
        return sum(g['inverter_count'] for g in self.inverter_groups)

    @property
    def all_inverter_ids(self) -> List[str]:
        """Get list of all inverter IDs"""
        ids = []
        for group in self.inverter_groups:
            ids.extend(group['inverter_ids'])
        return ids

    def get_inverter_group(self, inverter_id: str) -> Optional[str]:
        """Get the group ID for a given inverter"""
        for group in self.inverter_groups:
            if inverter_id in group['inverter_ids']:
                return group['group_id']
        return None

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> 'PlantConfig':
        """Load and parse plant configuration"""
        config = load_plant_config(config_path)
        plant = config.get('plant', {})

        return cls(
            plant_id=plant.get('plant_id', ''),
            plant_name=plant.get('plant_name', ''),
            total_capacity_kwp=plant.get('total_capacity_kwp', 1260.0),
            total_surface_m2=plant.get('total_surface_m2', 1400.0),
            inverter_groups=config.get('inverter_groups', []),
            inverter_specs=InverterSpecs.from_config(config),
            poi_meter_specs=POIMeterSpecs.from_config(config),
            thresholds=Thresholds.from_config(config),
            raw_config=config
        )


# Singleton instance for easy access
_plant_config: Optional[PlantConfig] = None


def get_plant_config(reload: bool = False) -> PlantConfig:
    """Get the plant configuration singleton"""
    global _plant_config
    if _plant_config is None or reload:
        _plant_config = PlantConfig.load()
    return _plant_config


# Convenience exports
def get_inverter_specs() -> InverterSpecs:
    """Get inverter specifications"""
    return get_plant_config().inverter_specs


def get_poi_meter_specs() -> POIMeterSpecs:
    """Get POI meter specifications"""
    return get_plant_config().poi_meter_specs


def get_thresholds() -> Thresholds:
    """Get alert thresholds"""
    return get_plant_config().thresholds


def get_all_inverter_ids() -> List[str]:
    """Get list of all inverter IDs"""
    return get_plant_config().all_inverter_ids


def get_plant_id() -> str:
    """Get the plant UUID"""
    return get_plant_config().plant_id
