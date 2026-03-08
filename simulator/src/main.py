import app.simulator as simulator
import tkinter as tk

if __name__ == "__main__":
    
    root = tk.Tk()
    app = simulator.PVDataSimulatorApp(root)
    root.mainloop()