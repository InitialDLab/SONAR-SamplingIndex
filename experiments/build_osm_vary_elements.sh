#!/bin/bash
#Copyright 2017 InitialDLab
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of
#this software and associated documentation files (the "Software"), to deal in
#the Software without restriction, including without limitation the rights to
#use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
#of the Software, and to permit persons to whom the Software is furnished to do
#so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.


read_file="raw/osm_data1_lat-lon-uid-date-id.osm"
base_file="raw/osm_full.osm"

mv -v $read_file $base_file

for elements in 100000000 250000000 500000000 750000000 1000000000 1250000000 1500000000 1750000000 2000000000
do
	rm -v $read_file
	echo "copying $elements from $base_file to $read_file for the next experiment"
	head -n $elements $base_file > $read_file
	echo "finished with copy"
	./exp build_osm_single
done

# next, we do the experiment with the full dataset
rm -v $read_file
echo "doing experiment with all elements"
mv -v $base_file $read_file
./exp build_osm_single
echo "done"