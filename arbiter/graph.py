"""
An implementation for a (possibly acyclic) directed graph.
"""
__all__ = ('DirectedGraph',)


from collections import namedtuple


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
                if parent in self._nodes and self.is_ancestor(
                        parent, name, visited=visited):
                    raise ValueError(parent)

        # Node safe to add

        if is_stub:
            self._stubs.remove(name)
            self._roots.remove(name)

        if parents:
            # Add stubs for any non-existent parent nodes
            for parent in parents:
                if parent not in self._nodes:
                    self._nodes[parent] = Node(
                        name=parent,
                        children=set((name,)),
                        parents=frozenset(),
                    )
                    self._stubs.add(parent)
                    self._roots.add(parent)
        else:
            self._roots.add(node)

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
            node = self._nodes[name]

            for parent_name in node.parents:
                self._nodes[parent_name].children.remove(current)

            if current in self._stubs:
                self._stubs.remove(current)
                self._roots.remove(current)
            elif current in self._roots:
                self._roots.remove(current)

            removed.add(current)

            if remove_children:
                stack.extend(
                    child for child in node.children if child not in removed
                )
            else:
                for child in node.children:
                    self._nodes[child].parents.remove()

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

        An exception will be raised if the node doesn't exist.
        """
        return frozenset(self._nodes[node].children)

    def get_parents(self, node):
        """
        Get the set of parents a node has.

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
        """
        if visited is None:
            visited = set()

        stack = [ancestor]

        while stack:
            current = stack.pop()

            if current == node:
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

        Returns the set of non-stub nodes which were removed.
        """
        pruned = set()

        while self._stubs:
            name = self._stubs.pop()
            self._roots.remove(name)

            for child in self._nodes.pop(name).children:
                if child not in pruned:
                    pruned.updated(
                        self._remove_node(child, remove_children=True)
                    )

        return pruned

    def __contains__(self, node):
        """
        Check whether a node is in the graph
        """
        return node in self._nodes
