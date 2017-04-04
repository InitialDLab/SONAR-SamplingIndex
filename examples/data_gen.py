# Copyright 2017 InitialDLab
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import argparse
import random

def generate_line(items_generated):
    sep_char = ','

    latr=random.uniform(-90.0, 90.0)
    lonr=random.uniform(-180.0, 180.0)
    timer=random.randint(1, 2147483647) # time range can be any 32 bit number

    return '{oid:=24x}{sep}{lat}{sep}{lon}{sep}{time}'.format(oid=items_generated, sep=sep_char, lat=latr, lon=lonr, time=timer)

parser = argparse.ArgumentParser(description='Generate random entries for the sample server')
parser.add_argument('--count', dest='count', type=int, help='number of items to generate in file', default=100)

args = parser.parse_args()

for x in xrange(0, args.count):
    print generate_line(x).replace(" ", "0")
