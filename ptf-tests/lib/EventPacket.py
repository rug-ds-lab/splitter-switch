import sys
#  pip3 install scapy

from ptf.testutils import *
import ptf.dataplane as dataplane

from scapy.all import *

try:
    import scapy.config
    import scapy.route
    import scapy.layers.l2
    import scapy.layers.inet
    import scapy.main
    from scapy.all import Packet
except ImportError:
    sys.exit("Need to install scapy for packet parsing")

class Event(Packet):
    name = "EventHeader"
    fields_desc = [
        ShortField("type", 0),
        BitField("timestamp", 0, 32)
    ]
