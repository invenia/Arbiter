"""
An implementation for a (possibly acyclic) directed graph.
"""
from collections import namedtuple


__all__ = ('DirectedGraph',)


Node = namedtuple('Node', ('name', 'children', 'parents'))


class DirectedGraph(object):
    """
    A directed graph with optional cycle prevention.
    """
    def __init__(self, acyclic=False):
        self._nodes = {}

        self._stubs = set()
        self._roots = set()

        self._acyclic = acyclic

    def add_node(self, name, parents=None):
        """
        add a node to the graph.

        Raises an exception if the node cannot be added (i.e., if a node
        that name already exists, or if it would create a cycle in an
        acyclic graph).

        NOTE: A node can be added before its parents are added.

        name: The name of the node to add to the graph.
        parents: (optional, None) The name of the nodes parents.
        """
        parents = set(parents or ())
        is_stub = False

        if name in self._nodes:
            if name in self._stubs:
                node = Node(name, self._nodes[name].children, parents)
                is_stub = True
            else:
                raise ValueError(name)
        else:
            node = Node(name, set(), parents)

        if self._acyclic:
            visited = set()

            for parent in parents:
                if parent in self._nodes:
                    if self.is_ancestor(parent, name, visited=visited):
                        raise ValueError(parent)
                elif parent == name:
                    raise ValueError(parent)

        # Node safe to add

        if is_stub:
            self._stubs.remove(name)

        if parents:
            for parent in parents:
                if parent == name:  # cycle
                    node.children.add(name)
                else:
                    parent_node = self._nodes.get(parent)

                    if parent_node:
                        parent_node.children.add(name)
                    else:  # add stub
                        self._nodes[parent] = Node(
                            name=parent,
                            children=set((name,)),
                            parents=frozenset(),
                        )
                        self._stubs.add(parent)
        else:
            self._roots.add(name)

        self._nodes[name] = node

    def remove_node(self, name, remove_children=False):
        """
        Remove a node from the graph. Returns the set of nodes
        that were removed.

        If the node doesn't exist, an exception will be raised.

        name: The name of the node to remove.
        remove_children: (optional, False) Whether to recursively remove
            any children of the node.
        """
        removed = set()

        stack = [name]

        while stack:
            current = stack.pop()
            node = self._nodes.pop(current)

            for parent_name in node.parents:
                parent_node = self._nodes[parent_name]

                parent_node.children.remove(current)

                if parent_name in self._stubs and not parent_node.children:
                    stack.append(parent_name)

            if current in self._stubs:
                self._stubs.remove(current)
            elif current in self._roots:
                self._roots.remove(current)

            removed.add(current)

            if remove_children:
                for child in node.children:
                    child_node = self._nodes[child]

                    child_node.parents.remove(current)

                    stack.append(child)
            else:
                for child in node.children:
                    child_node = self._nodes[child]

                    child_node.parents.remove(current)

                    if not child_node.parents:
                        self._roots.add(child)

        return removed

    @property
    def nodes(self):
        """
        The set of nodes in the graph.
        """
        return frozenset(self._nodes)

    @property
    def roots(self):
        """
        The set of nodes in the graph which have no parents.
        """
        return frozenset(self._roots)

    def get_children(self, node):
        """
        Get the set of children a node has.

        node: The name of the node.

        An exception will be raised if the node doesn't exist.
        """
        return frozenset(self._nodes[node].children)

    def get_parents(self, node):
        """
        Get the set of parents a node has.

        node: The name of the node.

        An exception will be raised if the node doesn't exist.
        """
        return frozenset(self._nodes[node].parents)

    def is_ancestor(self, node, ancestor, visited=None):
        """
        Check whether a node has another node as an ancestor.

        node: The name of the node being checked.
        ancestor: The name of the (possible) ancestor node.
        visited: (optional, None) If given, a set of nodes that have
            already been traversed. NOTE: The set will be updated with
            any new nodes that are visited.

        NOTE: If node doesn't exist, the method will return False.
        """
        if visited is None:
            visited = set()

        actual_node = self._nodes.get(node)

        if actual_node is None or node not in self._nodes:
            return False

        stack = list(actual_node.parents)

        while stack:
            current = stack.pop()

            if current == ancestor:
                return True

            if current not in visited:
                visited.add(current)

                actual_node = self._nodes.get(current)
                if actual_node:
                    stack.extend(actual_node.parents)

        return False

    def prune(self):
        """
        Remove any tasks that have stubs as ancestors (and the stubs
        themselves).

        Returns the set of nodes which were removed.
        """
        pruned = set()

        while self._stubs:
            stub = next(iter(self._stubs))  # get an arbitrary stub

            pruned.update(
                self.remove_node(stub, remove_children=True) - set((stub,))
            )

        return pruned

    def __contains__(self, node):
        """
        Check whether a node is in the graph
        """
        return node in self._nodes

    def __eq__(self, other):
        """
        Equality checking
        """
        if not isinstance(other, DirectedGraph):
            return NotImplemented

        return self._nodes == other._nodes and self._stubs == other._stubs

    def __ne__(self, other):
        """
        Inequality checking
        """
        if not isinstance(other, DirectedGraph):
            return NotImplemented

        return not (self == other)
