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

    # collect all previous, which includes the "previous_index"
    # and also the "--plugininstances" parameter value
    p['all_previous'] = set()

    # every non-root member has at least one previous index
    p['all_previous'].add(p['previous_index'])
    # ts-plugins might have more than one.
    for param in p['plugin_parameter_defaults']:
        if param['name'] == 'plugininstances':
            p['all_previous'].update(map(int, param['default'].split(',')))


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
        print([p['list_index'] for p in bfs_order])
        sys.exit(1)
    previous_index = queue.pop(0)

    children = [p for p in unverified_json_data['plugin_tree'] if p['previous_index'] == previous_index]
    # now we need to add all the children to bfs_order.
    # however there might be dependency relationships between
    # the children themselves, so we need to watch out for that.
    children_who_are_dependent_on_siblings = []

    while children:
        num_children = len(children)
        for child in children:
            visited_indexes = set(p['list_index'] for p in bfs_order)
            if child['all_previous'] <= visited_indexes:
                # Here is where child is being "visited" by BFS.
                # We are adding child to the list `bfs_order` in BFS-order.
                # If we wanted to, we could also create a plugin instance of "child" instead.
                # In effect, that would be the equivalent of saving the list `bfs_order` to
                # CUBE's database instead of a local variable in Python.
                bfs_order.append(child)
                queue.append(child['list_index'])
            else:
                children_who_are_dependent_on_siblings.append(child)

        # keep retrying until all children have been added.
        # if no children can be added, we're stuck either because of a cycle or a
        # parent which doesn't belong to this tree.
        if num_children == len(children_who_are_dependent_on_siblings):
            print('pipeline is invalid: a previous is unvisited, possibly cyclic or has '
                  'a parent which is not part of this pipeline!')
            sys.exit(1)
        children = children_who_are_dependent_on_siblings
        children_who_are_dependent_on_siblings = []


print('pipeline is valid! it is a single, connected, directed acyclic graph.')
