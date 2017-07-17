#!/usr/bin/env python3

import subprocess
import re
import time
import os
import sys

from collections import deque

measurement_stack = deque()
t_base = 0
headerInformation = {
	'parsed': False,
	'nodeIds':[],
	'node_row': "",
	'title_row': ""
}

def get_csv_numastat(pid):
	# get raw numastat output
	t = time.time()
	command = ['numastat', '-p', str(pid)] 
	output  = subprocess.check_output(command, stderr=subprocess.STDOUT)
	output  = str(output, 'utf-8')
	# transformation actually happens from here
	output_lines = output.split('\n')

	# pop away the ---- delimiters
	output_lines.pop(1)
	output_lines.pop(2)
	output_lines.pop(6)
	csv_lines = []

	# reformat data for easier usage
	for line in output_lines:
		line = re.sub("Node ", "", line)
		line = re.sub("\s+", ",", line)
		line_list = line.split(',')
		line_list = list(filter(None, line_list))
		csv_lines.append(line_list)

	csv_lines[0] = t
	measurement_stack.append(csv_lines)

def transform_measurement():
	global headerInformation
	measurement = measurement_stack.popleft()
	t_m = measurement.pop(0)

	if not headerInformation['parsed']:
		# reapeat node id for 4 times
		headerInformation['parsed'] = True
		header = "node_id,"

		for index, node_name in enumerate(measurement[0]):
			header += ",".join([node_name] * 4)
			if index != len(measurement[0])-1:
				header += ","

		headerInformation['node_row']  = header
		headerInformation['nodeIds']   = measurement[0][0:len(measurement[0])] #-1 to remove Total column
		headerInformation['title_row'] = "timestamp," + ",".join([measurement[1][0],
									 measurement[2][0],
									 measurement[3][0],
									 measurement[4][0]] * len(headerInformation['nodeIds']))

	res = [str(t_m - t_base)]
	line_format = "{0},{1},{2},{3}"

	for index, node_id in enumerate(headerInformation['nodeIds']):
		res.append(line_format.format(measurement[1][index+1], measurement[2][index+1],
					      measurement[3][index+1], measurement[4][index+1]))

	return ",".join(res)

if len(sys.argv) < 2:
	print("Please specify a program and its arguments to run")
	sys.exit(1)

# fork the process here
t_base = time.time()
p = subprocess.Popen(sys.argv[1:])

# continuously collect NUMA data
numamon_lines = []

while subprocess.Popen.poll(p) == None:
	get_csv_numastat(p.pid)
	numamon_lines.append(transform_measurement())
	time.sleep(0.001)

print(headerInformation['node_row'])
print(headerInformation['title_row'])

for line in numamon_lines:
	print(line)
