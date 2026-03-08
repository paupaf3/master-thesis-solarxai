import tkinter as tk
from tkinter import ttk
from datetime import datetime
from entities.simulation_params import SimulationParams
from data.pv_data_simulator import PVDataSimulator
from messages.message_producer_service import MessageProducerService
import threading
import uuid


class PVDataSimulatorApp:
    
    
    def __init__(self, root):
        self.root = root
        self.root.title("PV And Meteo Data Simulator")

        # Create text_output early so append_text can be used during initialization
        self.text_output = tk.Text(root, height=14, width=120)

        self.guid = str(uuid.uuid4())
        print(f"App using GUID: {self.guid}")
        self.message_producer = MessageProducerService(guid=self.guid)
        self.data_simulator = PVDataSimulator(self.append_text, self.on_simulation_end, self.message_producer.send_message)

        # Input frame
        input_frame = ttk.Frame(root)
        input_frame.pack(pady=10)

        # Start date of simulation
        ttk.Label(input_frame, text="Start Date (YYYY-MM-DD):").grid(row=0, column=0, padx=10, sticky="e")
        self.start_date_entry = ttk.Entry(input_frame, width=15)
        self.start_date_entry.grid(row=0, column=1, padx=5)
        self.start_date_entry.insert(0, "2025-01-01")

        # End date of simulation
        ttk.Label(input_frame, text="End Date (YYYY-MM-DD):").grid(row=0, column=2, padx=10, sticky="e")
        self.end_date_entry = ttk.Entry(input_frame, width=15)
        self.end_date_entry.grid(row=0, column=3, padx=5)
        self.end_date_entry.insert(0, "2025-01-03")

        # Speed of the simulations
        ttk.Label(input_frame, text="Speed:").grid(row=0, column=4, padx=10, sticky="e")
        self.speed_var = tk.StringVar(value="x1")
        self.speed_selector = ttk.Combobox(input_frame, textvariable=self.speed_var, values=["x1", "x2", "x8", "x60", "x120", "x240", "x600"], width=15, state="readonly")
        self.speed_selector.grid(row=0, column=5, padx=5)
        
        # Probability (in percentage) of higher local consume than generated energy
        # Buy energry from market, above 50 most days buy, below 50 most days sell energy
        # By default set to 50, so half of the days will be buy and half sell
        ttk.Label(input_frame, text="Consume Above Generated Prob. (%):").grid(row=2, column=0, padx=10, sticky="e")
        self.consume_above_gen_day_prob_entry = ttk.Entry(input_frame, width=15)
        self.consume_above_gen_day_prob_entry.grid(row=2, column=1, padx=5)
        self.consume_above_gen_day_prob_entry.insert(0, "50.0")
        
        # Probability (in percentage) of cloudy days (radiation lowering randomly on some time intervals)
        # If 50, half the days, if 100, all days will be cloudy
        # If 0, no days will be cloudy
        ttk.Label(input_frame, text="Cloudy Day Probability (%):").grid(row=2, column=2, padx=10, sticky="e")
        self.cloudy_day_prob_entry = ttk.Entry(input_frame, width=15)
        self.cloudy_day_prob_entry.grid(row=2, column=3, padx=5)
        self.cloudy_day_prob_entry.insert(0, "20.0")
        
        # Probability of inverter failure (stop producing energy)
        # If 50, half the days, if 100, all days will have inverter failures
        # If 0, no days will have inverter failures
        ttk.Label(input_frame, text="Inverter Failure Probability (%):").grid(row=3, column=0, padx=10, sticky="e")
        self.inverter_failure_prob_entry = ttk.Entry(input_frame, width=15)
        self.inverter_failure_prob_entry.grid(row=3, column=1, padx=5)
        self.inverter_failure_prob_entry.insert(0, "1.0")

        # Aprox time of inverter failure (range)
        ttk.Label(input_frame, text="Inverter Failure Duration (mins):").grid(row=3, column=2, padx=10, sticky="e")
        self.inverter_failure_duration_entry = ttk.Entry(input_frame, width=15)
        self.inverter_failure_duration_entry.grid(row=3, column=3, padx=5)
        self.inverter_failure_duration_entry.insert(0, "1-30")
        
        # Probability of bad connection (stop receiving data for a period of time)
        # If 50, half the days, if 100, all days will have bad connection
        # If 0, no days will have bad connection
        ttk.Label(input_frame, text="Bad Connection Probability (%):").grid(row=4, column=0, padx=10, sticky="e")
        self.bad_connection_prob_entry = ttk.Entry(input_frame, width=15)
        self.bad_connection_prob_entry.grid(row=4, column=1, padx=5)
        self.bad_connection_prob_entry.insert(0, "5.0")
        # Aprox time of bad connection (range)
        ttk.Label(input_frame, text="Bad Connection Duration (secs):").grid(row=4, column=2, padx=10, sticky="e")
        self.bad_connection_duration_entry = ttk.Entry(input_frame, width=15)
        self.bad_connection_duration_entry.grid(row=4, column=3, padx=5)
        self.bad_connection_duration_entry.insert(0, "1-30")
        
        # Start/Stop buttons
        self.start_button = ttk.Button(root, text="Start", command=self.start_simulation, state=tk.NORMAL, width=20)
        self.start_button.pack(pady=5)

        self.stop_button = ttk.Button(root, text="Stop", command=self.stop_simulation, state=tk.DISABLED, width=15)
        self.stop_button.pack(pady=5)

        # Text output (widget created earlier, now pack it)
        self.text_output.pack(padx=10, pady=10)
        
        # Clear text button
        self.clear_button = ttk.Button(root, text="Clear Output", command=lambda: self.text_output.delete(1.0, tk.END), width=20)
        self.clear_button.pack(pady=5)


    def start_simulation(self):
        try:
            # Define simulation params object
            params = SimulationParams()
            
            # Set simulation params from entries and check all correct
            # Dates for simulation start and end, and verify end date is after start date
            params.start = datetime.strptime(self.start_date_entry.get(), "%Y-%m-%d")
            params.end = datetime.strptime(self.end_date_entry.get(), "%Y-%m-%d")
            
            if params.start > params.end:
                self.append_text("Error: Start date must be before end date.")
                return
            
            # Speed simulation multiplier
            speed_map = {"x1": 1, "x2": 2, "x8": 8, "x60": 60, "x120" : 120, "x240": 240, "x600": 600}
            params.speed = speed_map.get(self.speed_var.get(), 1)
            
            params.consume_above_gen_day_prob = float(self.consume_above_gen_day_prob_entry.get())
            params.cloud_day_prob = float(self.cloudy_day_prob_entry.get())
            params.inverter_failure_prob = float(self.inverter_failure_prob_entry.get())
            params.inverter_failure_min_duration = self.inverter_failure_duration_entry.get().split('-')[0]
            params.inverter_failure_max_duration = self.inverter_failure_duration_entry.get().split('-')[1]
            params.bad_connection_prob = float(self.bad_connection_prob_entry.get())
            params.bad_connection_min_duration = self.bad_connection_duration_entry.get().split('-')[0]            
            params.bad_connection_max_duration = self.bad_connection_duration_entry.get().split('-')[1]
            self._print_params_console(params)
            
        except Exception as e:
            self.append_text("Error: Wrong values from entries.")
            return

        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        
        self.data_simulator.set_params(params)
        threading.Thread(target=self.data_simulator.run, daemon=True).start()
    
    def _print_params_console(self, params: SimulationParams):
        print("\nstart")
        print(params.start)
        
        print("\nend")
        print(params.end)
        
        print("\nspeed")
        print(params.speed)
        
        print("\nconsume_above_gen_day_prob")
        print(params.consume_above_gen_day_prob)
        
        print("\ncloud_day_prob")
        print(params.cloud_day_prob)
        
        print("\ninverter_failure_prob")
        print(params.inverter_failure_prob)
        
        print("\ninverter_failure_min_duration")
        print(params.inverter_failure_min_duration)
        
        print("\ninverter_failure_max_duration")
        print(params.inverter_failure_max_duration)
        
        print("\nbad_connection_prob")
        print(params.bad_connection_prob)
        
        print("\nbad_connection_min_duration")
        print(params.bad_connection_min_duration)
        
        print("\nbad_connection_max_duration")
        print(params.bad_connection_max_duration)
            
        
    def on_simulation_end(self):
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)


    def stop_simulation(self):
        self.data_simulator.stop()
        self.on_simulation_end()
        

    def append_text(self, message):
        self.text_output.insert(tk.END, message + "\n")
        self.text_output.see(tk.END)
