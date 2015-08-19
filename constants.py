__author__ = 'Zakaria'

WINDOW_SIZE = 10
HEARTBEAT_INTERVAL = 10
CHECK_INTERVAL = 12
MINIMUM_NODES = 1

SOURCE_HOST = '127.0.0.1'
SOURCE_PORT = 12344

DATATYPE_R_STREAM = 'R'
DATATYPE_S_STREAM = 'S'
DATATYPE_JOIN = 'X'
DATATYPE_HEARTBEAT = 'H'
DATATYPE_WINDOW_SIZE = 'W'
DATATYPE_DELETE = 'D'
DATATYPE_PROTOCOL = 'P'


PUNCTUATION = '*'

INPUT_EXIT = "exit"
INPUT_START_STREAM = "stream"
INPUT_GET_NODE = "getnode"
INPUT_STORE_PROTOCOL = "protocol"
INPUT_STORE_PROTOCOL_BATCH = "batchprotocol"


SIGNAL_START_STREAM = "stream"
SIGNAL_GET_NODE = "getnode"
SIGNAL_STORE_PROTOCOL = "singleprotocol"
SIGNAL_STORE_PROTOCOL_BATCH = "batchprotocol"

