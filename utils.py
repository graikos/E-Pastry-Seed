import json
import logging
import socket
from os import environ
import math

# get configuration settings from params.json
with open("params.json") as f:
    params = json.load(f)

logging.basicConfig(
    format="%(threadName)s-%(levelname)s: %(message)s",
    level=environ.get("LOGLEVEL", params["logging"]["level"]),
)
log = logging.getLogger(__name__)

log.info("Loaded params")


def get_ip():
    """
    Returns the local IP address of the machine
    :return: the IP address
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except (socket.error, socket.timeout):
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def create_request(header_dict, body_dict):
    """
    Creates request from passed header and body
    :param header_dict: string of request type
    :param body_dict: dictionary of body
    :return:
    """
    request_dict = {"header": header_dict, "body": body_dict}
    request_msg = json.dumps(request_dict, indent=2)

    return request_msg


def haversine(coord1, coord2):
    """
    Calculates the Haversine distance between two sets of coordinates
    :param coord1: tuple containing first set of (latitude, longitude)
    :param coord2: tuple containing second set of (latitude, longitude)
    """
    # Radius of Earth in kilometers
    R = 6371.0

    lat1, lon1 = coord1
    lat2, lon2 = coord2

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance
