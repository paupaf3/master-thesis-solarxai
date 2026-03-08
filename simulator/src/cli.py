
import uuid
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from entities.simulation_params import SimulationParams
from data.pv_data_simulator import PVDataSimulator
from messages.message_producer_service import MessageProducerService

def main():
	parser = argparse.ArgumentParser(description="Run PVDataSimulator with custom parameters.")
	parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD). Default: today')
	parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD). Default: today + 3 months')
	parser.add_argument('--speed', type=str, default='x1', choices=["x1", "x2", "x8", "x60", "x120", "x240", "x600"], help='Simulation speed multiplier')
	parser.add_argument('--consume-above-gen-day-prob', type=float, default=50.0, help='Probability (%%) of higher local consume than generated energy')
	parser.add_argument('--cloudy-day-prob', type=float, default=20.0, help='Probability (%%) of cloudy days')
	parser.add_argument('--inverter-failure-prob', type=float, default=1.0, help='Probability (%%) of inverter failure')
	parser.add_argument('--inverter-failure-duration', type=str, default='1-30', help='Inverter failure duration range (mins), e.g. 1-30')
	parser.add_argument('--bad-connection-prob', type=float, default=5.0, help='Probability (%%) of bad connection')
	parser.add_argument('--bad-connection-duration', type=str, default='1-30', help='Bad connection duration range (secs), e.g. 1-30')
	parser.add_argument('--guid', type=str, default=None, help='Instance GUID (if not provided, will be randomized)')

	args = parser.parse_args()
	guid = args.guid if args.guid else str(uuid.uuid4())
	print(f"Using GUID: {guid}")

	# Default dates: now (exact datetime) and now + 3 months
	now = datetime.now()
	start_date = datetime.strptime(args.start, "%Y-%m-%d") if args.start else now
	end_date = datetime.strptime(args.end, "%Y-%m-%d") if args.end else now + relativedelta(months=3)

	params = SimulationParams()
	params.start = start_date
	params.end = end_date
	speed_map = {"x1": 1, "x2": 2, "x8": 8, "x60": 60, "x120": 120, "x240": 240, "x600": 600}
	params.speed = speed_map.get(args.speed, 1)
	params.consume_above_gen_day_prob = args.consume_above_gen_day_prob
	params.cloud_day_prob = args.cloudy_day_prob
	params.inverter_failure_prob = args.inverter_failure_prob
	inv_fail_dur = args.inverter_failure_duration.split('-')
	params.inverter_failure_min_duration = inv_fail_dur[0]
	params.inverter_failure_max_duration = inv_fail_dur[1] if len(inv_fail_dur) > 1 else inv_fail_dur[0]
	params.bad_connection_prob = args.bad_connection_prob
	bad_conn_dur = args.bad_connection_duration.split('-')
	params.bad_connection_min_duration = bad_conn_dur[0]
	params.bad_connection_max_duration = bad_conn_dur[1] if len(bad_conn_dur) > 1 else bad_conn_dur[0]

	print(f"Simulation period: {params.start.strftime('%Y-%m-%d %H:%M:%S')} to {params.end.strftime('%Y-%m-%d %H:%M:%S')}")

	def print_callback(msg):
		print(msg)

	def end_callback():
		print("Simulation finished.")
		message_producer.close()

	message_producer = MessageProducerService(guid)
	simulator = PVDataSimulator(print_callback, end_callback, message_producer.send_message)
	simulator.set_params(params)
	simulator.run()

if __name__ == "__main__":
	main()