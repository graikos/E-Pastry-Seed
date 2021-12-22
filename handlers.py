import utils

REQUEST_MAP = {
    "add_node": lambda event_queue, body, random_seed_choice: add_node(event_queue, body, random_seed_choice),
    "dead_node": lambda event_queue, body, random_seed_choice: dead_node(event_queue, body)
}

STATUS_OK = 200
STATUS_NOT_FOUND = 404

def add_node(event_queue, body, random_seed_choice):
    """
    Reads node IP and PORT from request body and puts it in event queue to be added in the main thread
    Returns response, consisting of IP and PORT of random seed choice
    :param event_queue: shared queue
    :param body: the request body
    :param random_seed_choice: random node from existing nodes in seed server
    :return: None
    """
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
