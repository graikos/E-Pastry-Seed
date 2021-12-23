import utils
from utils import log
from random import choice, randint

REQUEST_MAP = {
    "add_node": lambda event_queue, body, server: add_node(event_queue, body, server),
    "dead_node": lambda event_queue, body, server: dead_node(event_queue, body)
}

STATUS_OK = 200
STATUS_NOT_FOUND = 404

def add_node(event_queue, body, server):
    """
    Reads node IP and PORT from request body and puts it in event queue to be added in the main thread
    Returns response, consisting of IP and PORT of random seed choice
    :param event_queue: shared queue
    :param body: the request body
    :param server: the Server object
    :return: None
    """

    random_seed_choice = None
    conn_addr = body["ip"], body["port"]
    if conn_addr in server.nodes:
        if len(server.nodes) > 1:
            random_seed_index = randint(0, len(server.nodes) - 1)
            if server.nodes[random_seed_index] == conn_addr:
                random_seed_choice = server.nodes[(random_seed_index + 1)%len(server.nodes)]
            else:
                random_seed_choice = server.nodes[random_seed_index]
    else:
        try:
            random_seed_choice = choice(server.nodes)
        except IndexError:
            pass

    log.info(f"Got request for seed, sending {random_seed_choice}")
    event_queue.put((body["ip"], body["port"], 1))
    if random_seed_choice is None:
        status = STATUS_NOT_FOUND
        body = {}
    else:
        status = STATUS_OK
        body = {"ip": random_seed_choice[0], "port": random_seed_choice[1]}

    header = {"status": status, "type": "seed_node"}

    return utils.create_request(header, body)

def dead_node(event_queue, body):
    """
    Adds node whose ID and PORT are contained in request body to event queue, to be removed by the main thread
    :param event_queue: shared queue
    :param body: the request body
    :return: None
    """
    event_queue.put((body["ip"], body["port"], 0))

    header = {"status": STATUS_OK}

    return utils.create_request(header, {})
