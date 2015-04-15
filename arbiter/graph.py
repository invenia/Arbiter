"""
An implementation for an acyclic directed graph.
"""
from collections import namedtuple, Hashable
from enum import Enum


__all__ = ('Graph', 'Strategy')


Node = namedtuple('Node', ('name', 'children', 'parents'))

Strategy = Enum('Strategy', ('orphan', 'promote', 'remove'))


class Graph(object):
    """
    An acyclic directed graph.
    """
    def __init__(self):
        self._nodes = {}

        self._stubs = set()
        self._roots = set()

    def add(self, name, parents=None):
        """
        add a node to the graph.

        Raises an exception if the node cannot be added (i.e., if a node
        that name already exists, or if it would create a cycle.

        NOTE: A node can be added before its parents are added.

        name: The name of the node to add to the graph. Name can be any
            unique Hashable value.
        parents: (optional, None) The name of the nodes parents.
        """
        if not isinstance(name, Hashable):
            raise TypeError(name)

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

        # cycle detection
        visited = set()

        for parent in parents:
            if self.ancestor_of(parent, name, visited=visited):
                raise ValueError(parent)
            elif parent == name:
                raise ValueError(parent)

        # Node safe to add
        if is_stub:
            self._stubs.remove(name)

        if parents:
            for parent_name in parents:
                parent_node = self._nodes.get(parent_name)

                if parent_node is not None:
                    parent_node.children.add(name)
                else:  # add stub
                    self._nodes[parent_name] = Node(
                        name=parent_name,
                        children=set((name,)),
                        parents=frozenset(),
                    )
                    self._stubs.add(parent_name)
        else:
            self._roots.add(name)

        self._nodes[name] = node

    def remove(self, name, strategy=Strategy.promote):
        """
        Remove a node from the graph. Returns the set of nodes that were
        removed.

        If the node doesn't exist, an exception will be raised.

        name: The name of the node to remove.
        strategy: (Optional, Strategy.promote) What to do with children
            or removed nodes. The options are:

            orphan: remove the node from the child's set of parents.
            promote: replace the node with the the node's parents in the
                childs set of parents.
            remove: recursively remove all children of the node.
        """
        removed = set()

        stack = [name]

        while stack:
            current = stack.pop()
            node = self._nodes.pop(current)

            if strategy == Strategy.remove:
                for child_name in node.children:
                    child_node = self._nodes[child_name]

                    child_node.parents.remove(current)

                    stack.append(child_name)
            else:
                for child_name in node.children:
                    child_node = self._nodes[child_name]

                    child_node.parents.remove(current)

                    if strategy == Strategy.promote:
                        for parent_name in node.parents:
                            parent_node = self._nodes[parent_name]

                            parent_node.children.add(child_name)
                            child_node.parents.add(parent_name)

                    if not child_node.parents:
                        self._roots.add(child_name)

            if current in self._stubs:
                self._stubs.remove(current)
            elif current in self._roots:
                self._roots.remove(current)
            else:  # stubs and roots (by definition) don't have parents
                for parent_name in node.parents:
                    parent_node = self._nodes[parent_name]

                    parent_node.children.remove(current)

                    if parent_name in self._stubs and not parent_node.children:
                        stack.append(parent_name)

            removed.add(current)

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

    def children(self, name):
        """
        Get the set of children a node has.

        name: The name of the node.

        An exception will be raised if the node doesn't exist.
        """
        return frozenset(self._nodes[name].children)

    def parents(self, name):
        """
        Get the set of parents a node has.

        name: The name of the node.

        An exception will be raised if the node doesn't exist.
        """
        return frozenset(self._nodes[name].parents)

    def ancestor_of(self, name, ancestor, visited=None):
        """
        Check whether a node has another node as an ancestor.

        name: The name of the node being checked.
        ancestor: The name of the (possible) ancestor node.
        visited: (optional, None) If given, a set of nodes that have
            already been traversed. NOTE: The set will be updated with
            any new nodes that are visited.

        NOTE: If node doesn't exist, the method will return False.
        """
        if visited is None:
            visited = set()

        node = self._nodes.get(name)

        if node is None or name not in self._nodes:
            return False

        stack = list(node.parents)

        while stack:
            current = stack.pop()

            if current == ancestor:
                return True

            if current not in visited:
                visited.add(current)

                node = self._nodes.get(current)
                if node is not None:
                    stack.extend(node.parents)

        return False

    def prune(self):
        """
        Remove any tasks that have stubs as ancestors (and the stubs
        themselves).

        Returns the set of nodes which were removed.
        """
        pruned = set()
        stubs = frozenset(self._stubs)

        for stub in stubs:
            pruned.update(self.remove(stub, strategy=Strategy.remove))

        return pruned - stubs  # we're only returning actual nodes

    def __contains__(self, node):
        """
        Check whether a node is in the graph
        """
        return node in self._nodes

    def __eq__(self, other):
        """
        Equality checking
        """
        if not isinstance(other, Graph):
            return NotImplemented

        return self._nodes == other._nodes and self._stubs == other._stubs

    def __ne__(self, other):
        """
        Inequality checking
        """
        if not isinstance(other, Graph):
            return NotImplemented

        return not (self == other)
