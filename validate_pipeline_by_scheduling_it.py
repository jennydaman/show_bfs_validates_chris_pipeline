#!/usr/bin/env python
#
# Purpose: Decides whether a JSON file represents a valid ChRIS pipeline.
#          Here, validity means that the JSON's data is representable as a
#          single, connected, directed acyclic graph.
#          The data are assumed to be representable as a graph
#          (assumptions: JSON is well-formed and coherent to schema).

import sys
import json

# Read the JSON file, which we assume to be well-formed.
# But it may or may not be valid! That's what we're trying to find out :)
json_file_name = sys.argv[1]
with open(json_file_name) as f:
    unverified_json_data = json.load(f)


for i, p in enumerate(unverified_json_data['plugin_tree']):
    # we need to know for later the list index for every member of plugin_tree
    p['list_index'] = i

    # set Defaults for missing values
    if 'plugin_parameter_defaults' not in p:
        p['plugin_parameter_defaults'] = []


# We need to sort these plugins into breadth-first order.
# I'm not going to use an efficient algorithm here, I'm just going to do one
# which is hopefully easy to read.
bfs_order = []


def is_root(d):
    return d['previous_index'] is None


# Gotta start from the root
try:
    root = next(filter(is_root, unverified_json_data['plugin_tree']))
except StopIteration:
    print('pipeline is invalid: has no root!')
    sys.exit(1)

# Do the breadth-first search!
bfs_order.append(root)
queue = [0]
while len(bfs_order) < len(unverified_json_data['plugin_tree']):
    if not queue:
        print('pipeline is invalid: disconnected!')
        sys.exit(1)
    previous_index = queue.pop(0)
    for p in unverified_json_data['plugin_tree']:
        if p['previous_index'] == previous_index:
            bfs_order.append(p)
            queue.append(p['list_index'])


# Now we will "pretend to schedule" the pipeline in BFS order.

root = bfs_order.pop(0)
visited_indexes = set()
visited_indexes.add(root['list_index'])

for p in bfs_order:
    previous_indexes = set()

    # every non-root member has at least one previous index
    previous_indexes.add(p['previous_index'])
    # ts-plugins might have more than one.
    for param in p['plugin_parameter_defaults']:
        if param['name'] == 'plugininstances':
            previous_indexes.update(map(int, param['default'].split(',')))

    if not previous_indexes <= visited_indexes:  # check if is subset
        print('pipeline is invalid: a previous is unvisited, possibly cyclic or has '
              'a parent which is not part of this pipeline!')
        sys.exit(1)

    # having visited an item in BFS order, then adding it to the list "visited"
    # is comparable to having scheduled the item. Instead of maintaining the list
    # "visited" ourselves, we would be creating a plugin instance in CUBE's database,
    # and CUBE would do the above if-statement check for us!
    visited_indexes.add(p['list_index'])

print('pipeline is valid! it is a single, connected, directed acyclic graph.')
