"""
Tests for the graph module.
"""
from nose.tools import assert_equals, assert_true, assert_false, assert_raises


def test_add_node():
    """
    add a node to a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

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
    Remove a node from a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(
        graph.remove_node('node', transitive_parents=False),
        frozenset(('node',))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with child, unique stub parent
    assert_equals(
        graph.remove_node('bar', transitive_parents=False),
        frozenset(('bar', 'foo'))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset(('baz',)))

    # node with non-unique stub parent
    assert_equals(
        graph.remove_node('bravo', transitive_parents=False),
        frozenset(('bravo',))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'alpha', 'beta',)
        )
    )
    assert_equals(graph.roots, frozenset(('baz',)))

    # stub
    assert_equals(
        graph.remove_node('alpha', transitive_parents=False),
        frozenset(('alpha',))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'beta')
        )
    )
    assert_equals(graph.roots, frozenset(('baz', 'beta')))

    assert_raises(KeyError, graph.remove_node, 'fake')


def test_remove_node_transitively():
    """
    Remove a node (transitively making its parents its children's
    parents) from a Graph.
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add_node('aye')
    graph.add_node('insect')
    graph.add_node('bee', ('aye', 'insect'))
    graph.add_node('cee', ('bee',))
    graph.add_node('child', ('stub', 'stub2'))
    graph.add_node('grandchild', ('child',))

    assert_equals(
        graph.nodes,
        frozenset(
            (
                'aye', 'insect', 'bee', 'cee', 'child', 'stub', 'stub2',
                'grandchild',
            )
        )
    )

    assert_equals(graph.roots, frozenset(('aye', 'insect')))

    # two new parents
    assert_equals(graph.remove_node('bee'), frozenset(('bee',)))
    assert_equals(
        graph.nodes,
        frozenset(
            (
                'aye', 'insect', 'cee', 'child', 'stub', 'stub2', 'grandchild',
            )
        )
    )

    assert_equals(
        graph.roots,
        frozenset(('aye', 'insect'))
    )

    assert_equals(graph.get_children('aye'), frozenset(('cee',)))
    assert_equals(graph.get_children('insect'), frozenset(('cee',)))
    assert_equals(graph.get_parents('cee'), frozenset(('aye', 'insect')))

    # now with stubs
    assert_equals(graph.remove_node('child'), frozenset(('child',)))
    assert_equals(
        graph.nodes,
        frozenset(
            (
                'aye', 'insect', 'cee', 'stub', 'stub2', 'grandchild',
            )
        )
    )

    assert_equals(
        graph.roots,
        frozenset(('aye', 'insect'))
    )

    assert_equals(graph.get_children('stub'), frozenset(('grandchild',)))
    assert_equals(graph.get_children('stub2'), frozenset(('grandchild',)))
    assert_equals(
        graph.get_parents('grandchild'),
        frozenset(('stub', 'stub2'))
    )

    # delete a stub
    assert_equals(graph.remove_node('stub2'), frozenset(('stub2',)))
    assert_equals(
        graph.nodes,
        frozenset(
            (
                'aye', 'insect', 'cee', 'stub', 'grandchild',
            )
        )
    )

    assert_equals(
        graph.roots,
        frozenset(('aye', 'insect'))
    )

    assert_equals(graph.get_children('stub'), frozenset(('grandchild',)))
    assert_equals(graph.get_parents('grandchild'), frozenset(('stub',)))


def test_remove_node_and_children():
    """
    Remove a node (and its children) from a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(
        graph.remove_node('node', remove_children=True),
        frozenset(('node',))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with child, unique stub parent
    assert_equals(
        graph.remove_node('bar', remove_children=True),
        frozenset(('bar', 'foo', 'baz'))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset())

    # node with non-unique stub parent
    assert_equals(graph.remove_node('bravo'), frozenset(('bravo',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('alpha', 'beta')
        )
    )
    assert_equals(graph.roots, frozenset())

    # stub
    assert_equals(
        graph.remove_node('alpha', remove_children=True),
        frozenset(('alpha', 'beta'))
    )

    assert_equals(
        graph.nodes,
        frozenset()
    )

    assert_raises(KeyError, graph.remove_node, 'fake', remove_children=True)


def test_prune():
    """
    Prune a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add_node('node')
    graph.add_node('bar', ('foo',))
    graph.add_node('baz', ('bar',))
    graph.add_node('beta', ('alpha',))
    graph.add_node('bravo', ('alpha',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
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
            ('node',)
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    assert_equals(graph.prune(), frozenset())

    assert_equals(
        graph.nodes,
        frozenset(
            ('node',)
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))


def test_equality():
    """
    Graph equality
    """
    from arbiter.graph import Graph

    graph = Graph()

    assert_false(graph == 1)
    assert_true(graph != 0)

    other = Graph()

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


def test_naming():
    """
    Node names just need to be hashable.
    """
    from arbiter.graph import Graph

    graph = Graph()

    for name in (1, float('NaN'), 0, None, '', frozenset(), (), False, sum):
        graph.add_node('child1', frozenset((name,)))

        graph.add_node(name)

        assert_true(name in graph)
        assert_equals(graph.nodes, frozenset((name, 'child1')))
        assert_equals(graph.roots, frozenset((name,)))
        assert_equals(graph.get_children(name), frozenset(('child1',)))
        assert_equals(graph.get_parents(name), frozenset())
        assert_false(graph.is_ancestor(name, name))

        graph.add_node('child2', frozenset((name,)))
        assert_equals(
            graph.get_children(name),
            frozenset(('child1', 'child2'))
        )

        graph.remove_node(name)
        graph.remove_node('child1')
        graph.remove_node('child2')

        assert_equals(graph.nodes, frozenset())

    graph.add_node(None)
    graph.add_node('', parents=((),))
    graph.add_node((), parents=(frozenset(),))
    graph.add_node(frozenset())

    assert_equals(graph.nodes, frozenset((None, '', (), frozenset())))
    assert_equals(graph.roots, frozenset((None, frozenset())))

    graph.remove_node((), remove_children=True)

    assert_equals(graph.nodes, frozenset((None, frozenset())))
    assert_equals(graph.roots, frozenset((None, frozenset())))

    assert_raises(TypeError, graph.add_node, [])
    assert_raises(TypeError, graph.add_node, 'valid', parents=([],))
