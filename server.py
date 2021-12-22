import json
import time
import socket
import threading
from queue import Queue
from random import choice
# project files
import utils
from handlers import REQUEST_MAP

class Server:
	"""
	Defines the seed server
	"""

	params = None

	def __init__(self):
		"""
		Initializes a new node
		"""
		# get configuration settings from params.json
		with open("params.json") as f:
			Server.params = json.load(f)
		self.SERVER_ADDR = (socket.gethostname(), Server.params["host"]["port"])
		self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
			client.settimeout(Server.params["net"]["timeout"])
			try:
				client.connect(node_addr)
				# send poll request
				header = {"type": "poll"}
				client.sendall(utils.create_request(header, {}))
				# receive response, will always be OK
				client.recv(Server.params["net"]["data_size"])
			except (socket.error, socket.timeout):
				event_queue.put((node_addr[0], node_addr[1], 0))

			event_queue.put(1)


	def run(self):
		"""
		Runs the main event loop
		:return: None
		"""
		# bind server to IP
		self.server_socket.bind(self.SERVER_ADDR)
		self.server_socket.listen()

		# create threads to listen for connections and to send stabilize signal
		event_queue = Queue()

		# accept incoming connections
		connection_acceptor = threading.Thread(target=self.accept_connections, args=(self.server_socket, event_queue))
		connection_acceptor.daemon = True
		connection_acceptor.start()

		# timer to poll regularly
		timer = threading.Thread(target=self.poll_timer, args=(event_queue, Server.params["net"]["polling_delay"]))
		timer.daemon = True
		timer.start()

		while True:
			data = event_queue.get()

			# data == 0 is used for verifying if a random node is alive
			if not data:
				# node list is empty
				if not len(self.nodes):
					continue
				# pick next node in list and start thread to poll it
				node_addr = self.nodes[self.current_node_index]
				polling_thread = threading.Thread(target=Server.poll_node, args=(node_addr, event_queue))
				polling_thread.daemon = True
				polling_thread.start()
				continue

			# polling thread returned 1, meaning node is alive, so increment index
			if data == 1:
				self.current_node_index += 1
				self.current_node_index %= len(self.nodes)

			# if data is (IP, port, i) tuple, add (i == 1) or remove (i == 0) from self.nodes
			if len(data) == 3:
				# if node should be added
				if data[2]:
					self.nodes.append(data[:2])
				# if node should be removed
				else:
					try:
						self.nodes.remove(data[:2])
						self.current_node_index %= len(self.nodes)
					except ValueError:
						pass
				continue

			# else, data is connection
			try:
				random_seed_choice = choice(self.nodes)
			except IndexError:
				random_seed_choice = None
			connection_handler = threading.Thread(target=self.handle_connection, args=(event_queue, data, random_seed_choice))
			connection_handler.start()

	# Thread Methods
	@staticmethod
	def accept_connections(server, event_queue):
		"""
		Accepts a new connection on the passed socket and places it in queue
		:param server: the socket
		:param event_queue: shared queue
		:return: None
		"""
		while True:
			# accept connection and add to event queue for handling
			# main thread will start a new thread to handle it
			conn_details = server.accept()
			event_queue.put(conn_details)

	@staticmethod
	def handle_connection(event_queue, conn_details, random_seed_choice):
		"""
		Handles existing connection until it closes
		:param conn_details: connection details (connection, address)
		:param event_queue: shared queue
		:param random_seed_choice: node chosen randomly to be returned as seed
		:return: None
		"""
		connection, address = conn_details

		with connection:
			data = connection.recv(Server.params["net"]["data_size"]).decode()

			if not data:
				return

			data = json.loads(data)

			# select RPC handler according to RPC type
			response = REQUEST_MAP[data["header"]["type"]](event_queue, data["body"], random_seed_choice)

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
