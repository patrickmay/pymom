#!/usr/bin/env python3

import sys
import os
from pymom.PyMom import PyMom

# This class is a simple example of how to use PyMom.  It sends a
# message on the test.pymom.consume topic for processing by the
# PyMomTestHandler class.

if __name__ == "__main__":
    if len(sys.argv) == 3:
        pymom = PyMom()
        producer = pymom.producer('test.pymom.consume')
        producer.write(sys.argv[1],sys.argv[2])
        print("Message written.")
    else:
        print("Usage:  " + sys.argv[0] + " <id> <payload>")
        
    os._exit(0)
