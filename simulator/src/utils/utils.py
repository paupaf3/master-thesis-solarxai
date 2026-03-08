import math

def smooth_transition(prev, new, alpha=0.85):
        return alpha * prev + (1 - alpha) * new
    
    
def smooth_angle(prev_deg, new_deg, alpha=0.85):
    # Convertimos a vectores unitarios
    x_prev = math.cos(math.radians(prev_deg))
    y_prev = math.sin(math.radians(prev_deg))
    x_new = math.cos(math.radians(new_deg))
    y_new = math.sin(math.radians(new_deg))

    # Interpolamos los vectores
    x = alpha * x_prev + (1 - alpha) * x_new
    y = alpha * y_prev + (1 - alpha) * y_new

    # Convertimos de nuevo a ángulo en grados
    return (math.degrees(math.atan2(y, x))) % 360