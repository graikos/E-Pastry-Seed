import json
import time
import socket
import threading
from queue import Queue
# project files
import utils
from utils import log
from handlers import REQUEST_MAP

class Server:
	"""
	Defines the seed server
	"""

	def __init__(self):
		"""
		Initializes a new node
		"""
		self.SERVER_ADDR = ("", utils.params["host"]["port"])
		self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# list holding alive nodes
		self.nodes = []
		self.current_node_index = 0

	@staticmethod
	def poll_node(node_addr, event_queue):
		"""
		Polls node with given address, puts address in shared queue (to be removed) if node is dead, or 1 if it is alive
		:param node_addr: (IP, port) of peer
		:param event_queue: shared queue
		:return: None
		"""
		log.info(f"Polling node: {node_addr}")
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
			client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			client.settimeout(utils.params["net"]["timeout"])
			try:
				client.connect(node_addr)
				# send poll request
				header = {"type": "poll"}
				client.sendall(utils.create_request(header, {}).encode())
				# receive response, will always be OK
				client.recv(utils.params["net"]["data_size"])
				event_queue.put(1)
				log.info("Node is alive")
			except (socket.error, socket.timeout):
				event_queue.put((node_addr[0], node_addr[1], 0))
				log.info("Node is dead")



	def run(self):
		"""
		Runs the main event loop
		:return: None
		"""
		log.info(f"Starting node on {self.SERVER_ADDR[0]}:{self.SERVER_ADDR[1]}")
		# bind server to IP
		self.server_socket.bind(self.SERVER_ADDR)
		self.server_socket.listen()

		# create threads to listen for connections and to send stabilize signal
		event_queue = Queue()

		# accept incoming connections
		connection_listener = threading.Thread(target=self.accept_connections, args=(self, event_queue))
		connection_listener.name = "Connection Listener"
		connection_listener.daemon = True
		connection_listener.start()

		# timer to poll regularly
		timer = threading.Thread(target=self.poll_timer, args=(event_queue, utils.params["net"]["polling_delay"]))
		timer.name = "Timer"
		timer.daemon = True
		timer.start()

		while True:
			data = event_queue.get()

			log.debug(f"Popped {data} from event queue")

			# data == 0 is used for verifying if a random node is alive
			if not data:
				log.debug(f"Current nodes: {self.nodes}")
				# node list is empty
				if not len(self.nodes):
					continue
				# pick next node in list and start thread to poll it
				node_addr = self.nodes[self.current_node_index]
				polling_thread = threading.Thread(target=Server.poll_node, args=(node_addr, event_queue))
				polling_thread.name = "Poll"
				polling_thread.daemon = True
				polling_thread.start()
				continue

			# polling thread returned 1, meaning node is alive, so increment index
			if data == 1:
				self.current_node_index += 1
				if len(self.nodes):
					self.current_node_index %= len(self.nodes)
				else:
					self.current_node_index = 0
				continue
			
			# if data is (IP, port, i) tuple, add (i == 1) or remove (i == 0) from self.nodes
			if len(data) == 3:
				# if node should be added
				if data[2]:
					conn = data[:2]
					# only append if node is not already in list
					if conn not in self.nodes:
						log.info(f"Adding {conn} to queue")
						self.nodes.append(conn)
				# if node should be removed
				else:
					try:
						conn = data[:2]
						log.info(f"Removing {conn}")
						self.nodes.remove(conn)
						if len(self.nodes):
							self.current_node_index %= len(self.nodes)
						else:
							self.current_node_index = 0
					except ValueError:
						pass

	# Thread Methods
	@staticmethod
	def accept_connections(server, event_queue):
		"""
		Accepts a new connection on the passed socket and places it in queue
		:param server: the Server object
		:param event_queue: shared queue
		:return: None
		"""
		while True:
			# accept connection and add to event queue for handling
			# main thread will start a new thread to handle it
			conn_details = server.server_socket.accept()
			log.debug(f"Got new connection from {conn_details[1]}")

			connection_handler = threading.Thread(target=server.handle_connection,
												  args=(event_queue, conn_details, server))
			connection_handler.start()

	@staticmethod
	def handle_connection(event_queue, conn_details, server):
		"""
		Handles existing connection until it closes
		:param conn_details: connection details (connection, address)
		:param event_queue: shared queue
		:param server: the Server object
		:return: None
		"""
		connection, address = conn_details

		with connection:
			data = connection.recv(utils.params["net"]["data_size"]).decode()

			if not data:
				return

			data = json.loads(data)

			# select RPC handler according to RPC type
			log.debug(f"Got RPC call of type: {data['header']['type']}")
			response = REQUEST_MAP[data["header"]["type"]](event_queue, data["body"], server)

			connection.sendall(response.encode())

	@staticmethod
	def poll_timer(event_queue, delay):
		"""
		Sleeps for specified amount of time, then places 0 in queue
		:param event_queue: shared queue
		:param delay: amount of time to sleep for
		:return: None
		"""
		while True:
			time.sleep(delay)
			event_queue.put(0)


if __name__ == "__main__":
	seed_server = Server()
	seed_server.run()
