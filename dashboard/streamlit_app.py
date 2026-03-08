"""
SolarX.ai Monitoring Dashboard
Minimalistic real-time monitoring with ML inference insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from utils import db_utils

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="SolarX.ai Monitoring",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =============================================================================
# CUSTOM STYLING
# =============================================================================
st.markdown("""
<style>
    /* Section headers */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #4FC3F7;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
        border-bottom: 2px solid #4FC3F7;
        padding-bottom: 0.3rem;
    }
    
    /* Alert styling */
    .alert-row {
        padding: 0.5rem 0;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        font-size: 0.9rem;
    }
    
    .alert-time {
        color: #aaa;
        font-family: monospace;
    }
    
    .alert-inverter {
        font-weight: 600;
        color: #4FC3F7;
    }
    
    .alert-message {
        color: #ccc;
    }
    
    /* Remove extra padding */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def format_kpi(value, unit="", decimals=1):
    """Format KPI values with appropriate scaling"""
    if pd.isna(value):
        return "N/A"
    if abs(value) >= 1000:
        return f"{value/1000:.{decimals}f}M{unit}" if abs(value) >= 1000000 else f"{value:.{decimals}f}{unit}"
    return f"{value:.{decimals}f}{unit}"


def calculate_delta(current, previous):
    """Calculate percentage delta between two values"""
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return None
    return ((current - previous) / previous) * 100


def _remove_tz(series):
    """Remove timezone info from datetime series (handles both tz-aware and naive)."""
    ts = pd.to_datetime(series)
    if ts.dt.tz is not None:
        return ts.dt.tz_localize(None)
    return ts


def create_power_chart(plant_summary, forecasts):
    """Create the main power output chart with forecast overlay per inverter (stacked area)"""
    fig = go.Figure()
    
    last_actual_ts = None
    if not plant_summary.empty:
        # Actual power
        plant_summary_copy = plant_summary.copy()
        plant_summary_copy['timestamp_hour'] = _remove_tz(plant_summary_copy['timestamp_hour'])
        last_actual_ts = plant_summary_copy['timestamp_hour'].max()
        
        fig.add_trace(go.Scatter(
            x=plant_summary_copy['timestamp_hour'],
            y=plant_summary_copy['total_power_kw'],
            name='Actual Total',
            line=dict(color='#4FC3F7', width=3),
            fill='tozeroy',
            fillcolor='rgba(79, 195, 247, 0.15)'
        ))
    
    if not forecasts.empty:
        # Get unique inverters and create stacked area for forecasts
        forecasts = forecasts.copy()
        forecasts['timestamp'] = _remove_tz(forecasts['timestamp'])
        
        # Round timestamps to 15-minute intervals for proper alignment across inverters
        forecasts['timestamp'] = forecasts['timestamp'].dt.floor('15min')
        
        # Aggregate by timestamp and inverter (take mean if duplicates after rounding)
        forecasts = forecasts.groupby(['timestamp', 'inverter_id'])['predicted_ac_power_kw'].mean().reset_index()
        
        # Sort by inverter_id for consistent stacking
        inverter_ids = sorted(forecasts['inverter_id'].unique())
        
        # Color palette for inverters
        colors = [
            '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', 
            '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9',
            '#F8B500', '#00CED1', '#FF7F50', '#9370DB'
        ]
        
        for i, inv_id in enumerate(inverter_ids):
            inv_data = forecasts[forecasts['inverter_id'] == inv_id].sort_values('timestamp')
            color = colors[i % len(colors)]
            # Convert hex to rgba for fillcolor
            hex_color = color.lstrip('#')
            r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
            fill_rgba = f'rgba({r}, {g}, {b}, 0.6)'
            
            fig.add_trace(go.Scatter(
                x=inv_data['timestamp'],
                y=inv_data['predicted_ac_power_kw'],
                name=f'FC {inv_id}',
                mode='lines',
                line=dict(width=0.5, color=color),
                stackgroup='forecast',
                fillcolor=fill_rgba,
                hovertemplate=f'{inv_id}: %{{y:.1f}} kW<extra></extra>'
            ))
        
        # Add vertical line to mark forecast boundary (where actual data ends)
        if last_actual_ts is not None:
            # Use add_shape instead of add_vline to avoid Plotly datetime annotation bug
            fig.add_shape(
                type="line",
                x0=last_actual_ts, x1=last_actual_ts,
                y0=0, y1=1,
                yref="paper",
                line=dict(color="#FFD700", width=2, dash="dash")
            )
            fig.add_annotation(
                x=last_actual_ts,
                y=1.05,
                yref="paper",
                text="Now → Forecast",
                showarrow=False,
                font=dict(color="#FFD700", size=10)
            )
    
    fig.update_layout(
        margin=dict(l=50, r=10, t=30, b=40),
        height=320,
        xaxis_title=None,
        yaxis_title="Power (kW)",
        legend=dict(
            orientation="h", 
            yanchor="bottom", 
            y=1.02, 
            xanchor="center", 
            x=0.5, 
            font=dict(color='#ccc', size=9),
            itemwidth=40
        ),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccc')
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#aaa'))
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#aaa'))
    
    return fig


def create_forecast_accuracy_chart(plant_summary, forecasts):
    """Create forecast accuracy comparison chart"""
    fig = go.Figure()
    
    if not plant_summary.empty and not forecasts.empty:
        # Merge actual and forecast data - normalize timestamps
        forecast_agg = forecasts.groupby('timestamp')['predicted_ac_power_kw'].sum().reset_index()
        forecast_agg.columns = ['timestamp', 'forecast_kw']
        forecast_agg['timestamp'] = _remove_tz(forecast_agg['timestamp']).dt.floor('H')
        
        actual_df = plant_summary[['timestamp_hour', 'total_power_kw']].copy()
        actual_df.columns = ['timestamp', 'actual_kw']
        actual_df['timestamp'] = _remove_tz(actual_df['timestamp']).dt.floor('H')
        
        merged = pd.merge(actual_df, forecast_agg, on='timestamp', how='inner')
        
        if not merged.empty:
            merged['error'] = merged['forecast_kw'] - merged['actual_kw']
            merged['error_pct'] = (merged['error'] / merged['actual_kw'].replace(0, 1)) * 100
            
            # Error bar chart
            colors = ['#EF5350' if x > 0 else '#66BB6A' for x in merged['error']]
            fig.add_trace(go.Bar(
                x=merged['timestamp'],
                y=merged['error'],
                name='Error',
                marker_color=colors
            ))
    
    fig.update_layout(
        margin=dict(l=50, r=10, t=10, b=40),
        height=200,
        xaxis_title=None,
        yaxis_title="Error (kW)",
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccc')
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#aaa'))
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', zeroline=True, zerolinecolor='rgba(255,255,255,0.3)', tickfont=dict(color='#aaa'))
    
    return fig


def create_inverter_performance_chart(inverters):
    """Create horizontal bar chart for inverter availability"""
    if inverters.empty:
        return go.Figure()
    
    # Get latest data per inverter
    latest = inverters.sort_values('timestamp_hour').groupby('inverter_id').last().reset_index()
    latest = latest.sort_values('inverter_id')
    
    # Color based on availability
    colors = ['#66BB6A' if x >= 90 else '#FFA726' if x >= 70 else '#EF5350' 
              for x in latest['availability_percent']]
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=latest['inverter_id'],
        x=latest['availability_percent'],
        orientation='h',
        marker_color=colors,
        text=[f"{x:.0f}%" for x in latest['availability_percent']],
        textposition='inside',
        textfont=dict(color='white', size=11)
    ))
    
    fig.update_layout(
        margin=dict(l=40, r=10, t=10, b=40),
        height=350,
        xaxis_title="Availability %",
        yaxis_title=None,
        xaxis=dict(range=[0, 105]),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccc')
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#aaa'))
    fig.update_yaxes(tickfont=dict(color='#ccc', size=11))
    
    return fig


def create_anomaly_heatmap(anomaly_predictions):
    """Create heatmap of reconstruction errors by inverter over time"""
    if anomaly_predictions.empty:
        return go.Figure()
    
    # Pivot data for heatmap
    pivot = anomaly_predictions.pivot_table(
        index='inverter_id',
        columns='timestamp',
        values='reconstruction_error',
        aggfunc='mean'
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale=[
            [0, '#1B5E20'],
            [0.3, '#66BB6A'],
            [0.6, '#FFA726'],
            [1, '#EF5350']
        ],
        showscale=True,
        colorbar=dict(title='Error', thickness=15, tickfont=dict(color='#aaa'))
    ))
    
    fig.update_layout(
        margin=dict(l=40, r=10, t=10, b=40),
        height=350,
        xaxis_title=None,
        yaxis_title=None,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccc')
    )
    fig.update_xaxes(tickfont=dict(color='#aaa'))
    fig.update_yaxes(tickfont=dict(color='#ccc', size=11))
    
    return fig


def create_environmental_chart(meteo):
    """Create dual-axis chart for irradiance and temperature"""
    if meteo.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    # Irradiance (primary y-axis)
    fig.add_trace(go.Scatter(
        x=meteo['timestamp_hour'],
        y=meteo['avg_irradiance_wm2'],
        name='Irradiance (W/m²)',
        line=dict(color='#FFB74D', width=2),
        fill='tozeroy',
        fillcolor='rgba(255, 183, 77, 0.2)'
    ))
    
    # Temperature (secondary y-axis)
    fig.add_trace(go.Scatter(
        x=meteo['timestamp_hour'],
        y=meteo['temp_avg_c'],
        name='Temperature (°C)',
        line=dict(color='#4FC3F7', width=2),
        yaxis='y2'
    ))
    
    fig.update_layout(
        margin=dict(l=50, r=50, t=40, b=40),
        height=250,
        xaxis_title=None,
        yaxis=dict(title="Irradiance (W/m²)", side='left', titlefont=dict(color='#FFB74D'), tickfont=dict(color='#aaa')),
        yaxis2=dict(title="Temp (°C)", side='right', overlaying='y', titlefont=dict(color='#4FC3F7'), tickfont=dict(color='#aaa')),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="center", x=0.5, font=dict(color='#ccc')),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ccc')
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)', tickfont=dict(color='#aaa'))
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
    
    return fig


# =============================================================================
# MAIN DASHBOARD
# =============================================================================

# Title
st.subheader("SOLARX.ai MONITORING")

# Controls row
col_plant, col_time, col_refresh = st.columns([3, 4, 1])

with col_plant:
    try:
        plants = db_utils.get_available_plants()
        plant_ids = [p['plant_id'] for p in plants] if plants else []
        selected_plant = st.selectbox("Plant", options=plant_ids, index=0 if plant_ids else None, label_visibility="collapsed")
    except Exception:
        selected_plant = None
        st.warning("No plants found")

with col_time:
    time_range = st.radio(
        "Time Range",
        options=["6h", "12h", "24h", "7d"],
        index=2,
        horizontal=True,
        label_visibility="collapsed"
    )
    lookback_hours = {"6h": 6, "12h": 12, "24h": 24, "7d": 168}[time_range]

with col_refresh:
    if st.button("Refresh", use_container_width=True):
        st.rerun()

st.divider()

# Load data
if selected_plant:
    try:
        data = db_utils.get_dashboard_data_with_inference(
            lookback_hours=lookback_hours,
            forecast_horizon_hours=72,  # Match inference engine's 3-day horizon
            plant_id=selected_plant
        )
        
        plant_summary = data['plant_summary']
        inverters = data['inverters']
        meteo = data['meteo']
        anomaly_preds = data['anomaly_predictions']
        forecast_preds = data['forecast_predictions']
        latest_anomalies = data['latest_anomalies']
        
        # =================================================================
        # KPI ROW
        # =================================================================
        if not plant_summary.empty:
            latest = plant_summary.iloc[0]
            previous = plant_summary.iloc[1] if len(plant_summary) > 1 else None
            
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            
            with kpi1:
                current_power = latest['total_power_kw']
                delta = calculate_delta(current_power, previous['total_power_kw'] if previous is not None else None)
                st.metric(
                    label="CURRENT POWER",
                    value=f"{current_power:.0f} kW" if pd.notna(current_power) else "N/A",
                    delta=f"{delta:+.1f}%" if delta else None
                )
            
            with kpi2:
                total_energy = plant_summary['total_generation_kwh'].sum()
                st.metric(
                    label="TOTAL ENERGY",
                    value=f"{total_energy/1000:.1f} MWh" if total_energy >= 1000 else f"{total_energy:.1f} kWh"
                )
            
            with kpi3:
                pr = latest['instantaneous_pr']
                st.metric(
                    label="PERF. RATIO",
                    value=f"{pr*100:.1f}%" if pd.notna(pr) else "N/A"
                )
            
            with kpi4:
                active = latest['active_inverters']
                total = latest['total_inverters']
                anomaly_count = len(latest_anomalies) if not latest_anomalies.empty else 0
                st.metric(
                    label="INVERTERS",
                    value=f"{active}/{total}",
                    delta=f"{anomaly_count} alerts" if anomaly_count > 0 else None,
                    delta_color="inverse"
                )
        
        st.divider()
        
        # =================================================================
        # POWER OUTPUT & FORECAST ACCURACY
        # =================================================================
        st.markdown('<p class="section-header">Power Output</p>', unsafe_allow_html=True)
        
        col_power, col_accuracy = st.columns([2, 1])
        
        with col_power:
            power_chart = create_power_chart(plant_summary, forecast_preds)
            st.plotly_chart(power_chart, use_container_width=True)
        
        with col_accuracy:
            st.markdown("**Forecast Accuracy**")
            if not forecast_preds.empty and not plant_summary.empty:
                # Calculate MAPE - normalize timestamps
                forecast_agg = forecast_preds.groupby('timestamp')['predicted_ac_power_kw'].sum().reset_index()
                forecast_agg.columns = ['timestamp', 'forecast_kw']
                forecast_agg['timestamp'] = _remove_tz(forecast_agg['timestamp']).dt.floor('H')
                
                actual_df = plant_summary[['timestamp_hour', 'total_power_kw']].copy()
                actual_df.columns = ['timestamp', 'actual_kw']
                actual_df['timestamp'] = _remove_tz(actual_df['timestamp']).dt.floor('H')
                
                merged = pd.merge(actual_df, forecast_agg, on='timestamp', how='inner')
                
                if not merged.empty and merged['actual_kw'].sum() > 0:
                    mape = (abs(merged['forecast_kw'] - merged['actual_kw']) / merged['actual_kw'].replace(0, 1)).mean() * 100
                    st.metric("MAPE", f"{mape:.1f}%")
                else:
                    st.info("No overlap data")
            else:
                st.info("No forecast data available")
            
            accuracy_chart = create_forecast_accuracy_chart(plant_summary, forecast_preds)
            st.plotly_chart(accuracy_chart, use_container_width=True)
        
        # =================================================================
        # INVERTER PERFORMANCE & ANOMALY DETECTION
        # =================================================================
        st.markdown('<p class="section-header">Inverter Status</p>', unsafe_allow_html=True)
        
        col_inverters, col_anomaly = st.columns(2)
        
        with col_inverters:
            st.markdown("**Availability by Inverter**")
            inv_chart = create_inverter_performance_chart(inverters)
            st.plotly_chart(inv_chart, use_container_width=True)
        
        with col_anomaly:
            st.markdown("**Anomaly Detection**")
            if not anomaly_preds.empty:
                anomaly_chart = create_anomaly_heatmap(anomaly_preds)
                st.plotly_chart(anomaly_chart, use_container_width=True)
            else:
                st.info("No anomaly data available")
        
        # =================================================================
        # ENVIRONMENTAL CONDITIONS
        # =================================================================
        st.markdown('<p class="section-header">Environmental Conditions</p>', unsafe_allow_html=True)
        
        env_chart = create_environmental_chart(meteo)
        st.plotly_chart(env_chart, use_container_width=True)
        
        # =================================================================
        # ALERTS TABLE
        # =================================================================
        st.markdown('<p class="section-header">Alerts</p>', unsafe_allow_html=True)
        
        alerts = []
        
        # Add anomaly alerts
        if not latest_anomalies.empty:
            for _, row in latest_anomalies.iterrows():
                alerts.append({
                    'timestamp': row['timestamp'],
                    'source': row['inverter_id'],
                    'type': 'Anomaly',
                    'message': f"Reconstruction error: {row['reconstruction_error']:.4f}"
                })
        
        # Add availability alerts from inverters
        if not inverters.empty:
            latest_inv = inverters.sort_values('timestamp_hour').groupby('inverter_id').last().reset_index()
            low_avail = latest_inv[latest_inv['availability_percent'] < 80]
            for _, row in low_avail.iterrows():
                alerts.append({
                    'timestamp': row['timestamp_hour'],
                    'source': row['inverter_id'],
                    'type': 'Availability',
                    'message': f"Low availability: {row['availability_percent']:.1f}%"
                })
        
        if alerts:
            alerts_df = pd.DataFrame(alerts)
            alerts_df = alerts_df.sort_values('timestamp', ascending=False).head(10)
            
            for _, alert in alerts_df.iterrows():
                ts = alert['timestamp'].strftime('%H:%M') if pd.notna(alert['timestamp']) else '--:--'
                st.markdown(
                    f'<div class="alert-row">'
                    f'<span class="alert-time">{ts}</span> | '
                    f'<span class="alert-inverter">{alert["source"]}</span> | '
                    f'<span class="alert-message">{alert["message"]}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
        else:
            st.success("No alerts - system operating normally")
        
        # =================================================================
        # FOOTER
        # =================================================================
        st.divider()
        
        footer_cols = st.columns(4)
        
        if not meteo.empty:
            latest_meteo = meteo.iloc[0]
            with footer_cols[0]:
                irr = latest_meteo['avg_irradiance_wm2']
                st.caption(f"Irradiance: {irr:.0f} W/m²" if pd.notna(irr) else "Irradiance: N/A")
            with footer_cols[1]:
                temp = latest_meteo['temp_avg_c']
                st.caption(f"Temperature: {temp:.1f}°C" if pd.notna(temp) else "Temperature: N/A")
        
        with footer_cols[2]:
            if not plant_summary.empty:
                latest_ts = plant_summary['timestamp_hour'].max()
                st.caption(f"Data: {latest_ts.strftime('%Y-%m-%d %H:%M')}" if pd.notna(latest_ts) else "")
        
        with footer_cols[3]:
            st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")
    
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.exception(e)
else:
    st.info("Select a plant to view monitoring data")
