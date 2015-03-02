"""
Tests for the graph module.
"""
from nose.tools import assert_equals, assert_true, assert_false, assert_raises


def test_add_node():
    """
    add a node to a DirectedGraph
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph()

    assert_equals(graph.nodes, frozenset())
    assert_equals(graph.roots, frozenset())
    assert_false('foo' in graph)

    graph.add_node('foo')

    assert_equals(graph.nodes, frozenset(('foo',)))
    assert_equals(graph.roots, frozenset(('foo',)))
    assert_true('foo' in graph)
    assert_equals(graph.get_children('foo'), frozenset())
    assert_equals(graph.get_parents('foo'), frozenset())
    assert_false(graph.is_ancestor('foo', 'foo'))

    graph.add_node('bar', ('foo', 'baz'))

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))

    assert_true('foo' in graph)
    assert_true('bar' in graph)
    assert_true('baz' in graph)

    assert_equals(graph.get_children('foo'), frozenset(('bar',)))
    assert_equals(graph.get_children('bar'), frozenset())
    assert_equals(graph.get_children('baz'), frozenset(('bar',)))

    assert_equals(graph.get_parents('foo'), frozenset())
    assert_equals(graph.get_parents('bar'), frozenset(('foo', 'baz')))
    assert_equals(graph.get_parents('baz'), frozenset())

    assert_false(graph.is_ancestor('foo', 'foo'))
    assert_false(graph.is_ancestor('foo', 'bar'))
    assert_false(graph.is_ancestor('foo', 'baz'))

    assert_true(graph.is_ancestor('bar', 'foo'))
    assert_false(graph.is_ancestor('bar', 'bar'))
    assert_true(graph.is_ancestor('bar', 'baz'))

    assert_false(graph.is_ancestor('baz', 'foo'))
    assert_false(graph.is_ancestor('baz', 'bar'))
    assert_false(graph.is_ancestor('baz', 'baz'))

    graph.add_node('baz', ('bar',))

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))

    assert_true('foo' in graph)
    assert_true('bar' in graph)
    assert_true('baz' in graph)

    assert_equals(graph.get_children('foo'), frozenset(('bar',)))
    assert_equals(graph.get_children('bar'), frozenset(('baz',)))
    assert_equals(graph.get_children('baz'), frozenset(('bar',)))

    assert_equals(graph.get_parents('foo'), frozenset())
    assert_equals(graph.get_parents('bar'), frozenset(('foo', 'baz')))
    assert_equals(graph.get_parents('baz'), frozenset(('bar',)))

    assert_false(graph.is_ancestor('foo', 'foo'))
    assert_false(graph.is_ancestor('foo', 'bar'))
    assert_false(graph.is_ancestor('foo', 'baz'))

    assert_true(graph.is_ancestor('bar', 'foo'))
    assert_true(graph.is_ancestor('bar', 'bar'))
    assert_true(graph.is_ancestor('bar', 'baz'))

    assert_true(graph.is_ancestor('baz', 'foo'))
    assert_true(graph.is_ancestor('baz', 'bar'))
    assert_true(graph.is_ancestor('baz', 'baz'))

    graph.add_node('ouroboros', ('ouroboros',))

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz', 'ouroboros')))
    assert_equals(graph.roots, frozenset(('foo',)))

    assert_true('ouroboros' in graph)
    assert_equals(graph.get_children('ouroboros'), frozenset(('ouroboros',)))
    assert_equals(graph.get_parents('ouroboros'), frozenset(('ouroboros',)))
    assert_true(graph.is_ancestor('ouroboros', 'ouroboros'))

    assert_raises(ValueError, graph.add_node, 'foo')


def test_add_node_acyclic():
    """
    add a node to an acyclic DirectedGraph
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph(acyclic=True)

    assert_equals(graph.nodes, frozenset())
    assert_equals(graph.roots, frozenset())
    assert_false('foo' in graph)

    graph.add_node('foo')

    assert_equals(graph.nodes, frozenset(('foo',)))
    assert_equals(graph.roots, frozenset(('foo',)))
    assert_true('foo' in graph)
    assert_equals(graph.get_children('foo'), frozenset())
    assert_equals(graph.get_parents('foo'), frozenset())
    assert_false(graph.is_ancestor('foo', 'foo'))

    graph.add_node('bar', ('foo', 'baz'))

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))

    assert_true('foo' in graph)
    assert_true('bar' in graph)
    assert_true('baz' in graph)

    assert_equals(graph.get_children('foo'), frozenset(('bar',)))
    assert_equals(graph.get_children('bar'), frozenset())
    assert_equals(graph.get_children('baz'), frozenset(('bar',)))

    assert_equals(graph.get_parents('foo'), frozenset())
    assert_equals(graph.get_parents('bar'), frozenset(('foo', 'baz')))
    assert_equals(graph.get_parents('baz'), frozenset())

    assert_false(graph.is_ancestor('foo', 'foo'))
    assert_false(graph.is_ancestor('foo', 'bar'))
    assert_false(graph.is_ancestor('foo', 'baz'))

    assert_true(graph.is_ancestor('bar', 'foo'))
    assert_false(graph.is_ancestor('bar', 'bar'))
    assert_true(graph.is_ancestor('bar', 'baz'))

    assert_false(graph.is_ancestor('baz', 'foo'))
    assert_false(graph.is_ancestor('baz', 'bar'))
    assert_false(graph.is_ancestor('baz', 'baz'))

    assert_raises(ValueError, graph.add_node, 'baz', ('bar',))
    assert_raises(ValueError, graph.add_node, 'ouroboros', ('ouroboros',))
    assert_raises(ValueError, graph.add_node, 'foo')

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))


def test_remove_node():
    """
    Remove a node from a DirectedGraph
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))
    graph.add_node('tick', ('tock',))
    graph.add_node('tock', ('tick',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta',
             'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(graph.remove_node('node'), frozenset(('node',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('foo', 'bar', 'baz', 'alpha', 'beta', 'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with child, unique stub parent
    assert_equals(graph.remove_node('bar'), frozenset(('bar', 'foo')))

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'alpha', 'beta', 'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('baz',)))

    # node with non-unique stub parent
    assert_equals(graph.remove_node('bravo'), frozenset(('bravo',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'alpha', 'beta', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('baz',)))

    # stub
    assert_equals(graph.remove_node('alpha'), frozenset(('alpha',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'beta', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('baz', 'beta')))

    # cycle
    assert_equals(graph.remove_node('tock'), frozenset(('tock',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'beta', 'tick')
        )
    )
    assert_equals(graph.roots, frozenset(('baz', 'beta', 'tick')))

    assert_equals(graph.get_parents('tick'), frozenset())
    assert_equals(graph.get_children('tick'), frozenset())

    assert_raises(KeyError, graph.remove_node, 'fake')


def test_remove_node_and_children():
    """
    Remove a node (and its children) from a DirectedGraph
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))
    graph.add_node('tick', ('tock',))
    graph.add_node('tock', ('tick',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta',
             'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(graph.remove_node('node', True), frozenset(('node',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('foo', 'bar', 'baz', 'alpha', 'beta', 'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with child, unique stub parent
    assert_equals(
        graph.remove_node('bar', True),
        frozenset(('bar', 'foo', 'baz'))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('alpha', 'beta', 'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with non-unique stub parent
    assert_equals(graph.remove_node('bravo'), frozenset(('bravo',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('alpha', 'beta', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset())

    # stub
    assert_equals(graph.remove_node('alpha'), frozenset(('alpha',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('beta', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('beta',)))

    # cycle
    assert_equals(graph.remove_node('tock'), frozenset(('tock',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('beta', 'tick')
        )
    )
    assert_equals(graph.roots, frozenset(('beta', 'tick')))

    assert_equals(graph.get_parents('tick'), frozenset())
    assert_equals(graph.get_children('tick'), frozenset())

    assert_raises(KeyError, graph.remove_node, 'fake')


def test_prune():
    """
    Prune a DirectedGraph
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))
    graph.add_node('tick', ('tock',))
    graph.add_node('tock', ('tick',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta',
             'bravo', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    assert_equals(
        graph.prune(),
        frozenset(('bar', 'baz', 'beta', 'bravo'))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    assert_equals(graph.prune(), frozenset())

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'tick', 'tock')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))


def test_equality():
    """
    DirectedGraph equality
    """
    from arbiter.graph import DirectedGraph

    graph = DirectedGraph()

    assert_false(graph == 1)
    assert_true(graph != 0)

    other = DirectedGraph()

    assert_true(graph == other)
    assert_false(graph != other)

    graph.add_node('foo')

    assert_false(graph == other)
    assert_true(graph != other)

    graph.add_node('bar', ('foo',))

    other.add_node('bar', ('foo',))

    # still shouldn't match graph['foo'] is a stub
    assert_false(graph == other)
    assert_true(graph != other)

    other.add_node('foo')

    assert_true(graph == other)
    assert_false(graph != other)
