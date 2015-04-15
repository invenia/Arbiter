"""
Tests for the graph module.
"""
from nose.tools import assert_equals, assert_true, assert_false, assert_raises


def test_add():
    """
    add a node to a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

    assert_equals(graph.nodes, frozenset())
    assert_equals(graph.roots, frozenset())
    assert_false('foo' in graph)

    graph.add('foo')

    assert_equals(graph.nodes, frozenset(('foo',)))
    assert_equals(graph.roots, frozenset(('foo',)))
    assert_true('foo' in graph)
    assert_equals(graph.children('foo'), frozenset())
    assert_equals(graph.parents('foo'), frozenset())
    assert_false(graph.ancestor_of('foo', 'foo'))

    graph.add('bar', ('foo', 'baz'))

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))

    assert_true('foo' in graph)
    assert_true('bar' in graph)
    assert_true('baz' in graph)

    assert_equals(graph.children('foo'), frozenset(('bar',)))
    assert_equals(graph.children('bar'), frozenset())
    assert_equals(graph.children('baz'), frozenset(('bar',)))

    assert_equals(graph.parents('foo'), frozenset())
    assert_equals(graph.parents('bar'), frozenset(('foo', 'baz')))
    assert_equals(graph.parents('baz'), frozenset())

    assert_false(graph.ancestor_of('foo', 'foo'))
    assert_false(graph.ancestor_of('foo', 'bar'))
    assert_false(graph.ancestor_of('foo', 'baz'))

    assert_true(graph.ancestor_of('bar', 'foo'))
    assert_false(graph.ancestor_of('bar', 'bar'))
    assert_true(graph.ancestor_of('bar', 'baz'))

    assert_false(graph.ancestor_of('baz', 'foo'))
    assert_false(graph.ancestor_of('baz', 'bar'))
    assert_false(graph.ancestor_of('baz', 'baz'))

    assert_raises(ValueError, graph.add, 'baz', ('bar',))
    assert_raises(ValueError, graph.add, 'ouroboros', ('ouroboros',))
    assert_raises(ValueError, graph.add, 'foo')

    assert_equals(graph.nodes, frozenset(('foo', 'bar', 'baz')))
    assert_equals(graph.roots, frozenset(('foo',)))


def test_remove_orphan():
    """
    Remove a node from a Graph
    """
    from arbiter.graph import Graph, Strategy

    graph = Graph()

    graph.add('node')
    graph.add('bar', ('foo',))
    graph.add('baz', ('bar',))
    graph.add('beta', ('alpha',))
    graph.add('bravo', ('alpha',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(
        graph.remove('node', strategy=Strategy.orphan),
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
        graph.remove('bar', strategy=Strategy.orphan),
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
        graph.remove('bravo', strategy=Strategy.orphan),
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
        graph.remove('alpha', strategy=Strategy.orphan),
        frozenset(('alpha',))
    )

    assert_equals(
        graph.nodes,
        frozenset(
            ('baz', 'beta')
        )
    )
    assert_equals(graph.roots, frozenset(('baz', 'beta')))

    assert_raises(KeyError, graph.remove, 'fake')


def test_remove_promote():
    """
    Remove a node (transitively making its parents its children's
    parents) from a Graph.
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add('aye')
    graph.add('insect')
    graph.add('bee', ('aye', 'insect'))
    graph.add('cee', ('bee',))
    graph.add('child', ('stub', 'stub2'))
    graph.add('grandchild', ('child',))

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
    assert_equals(graph.remove('bee'), frozenset(('bee',)))
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

    assert_equals(graph.children('aye'), frozenset(('cee',)))
    assert_equals(graph.children('insect'), frozenset(('cee',)))
    assert_equals(graph.parents('cee'), frozenset(('aye', 'insect')))

    # now with stubs
    assert_equals(graph.remove('child'), frozenset(('child',)))
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

    assert_equals(graph.children('stub'), frozenset(('grandchild',)))
    assert_equals(graph.children('stub2'), frozenset(('grandchild',)))
    assert_equals(
        graph.parents('grandchild'),
        frozenset(('stub', 'stub2'))
    )

    # delete a stub
    assert_equals(graph.remove('stub2'), frozenset(('stub2',)))
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

    assert_equals(graph.children('stub'), frozenset(('grandchild',)))
    assert_equals(graph.parents('grandchild'), frozenset(('stub',)))


def test_remove_remove():
    """
    Remove a node (and its children) from a Graph
    """
    from arbiter.graph import Graph, Strategy

    graph = Graph()

    graph.add('node')
    graph.add('bar', ('foo',))
    graph.add('baz', ('bar',))
    graph.add('beta', ('alpha',))
    graph.add('bravo', ('alpha',))

    assert_equals(
        graph.nodes,
        frozenset(
            ('node', 'foo', 'bar', 'baz', 'alpha', 'beta', 'bravo')
        )
    )
    assert_equals(graph.roots, frozenset(('node',)))

    # node with no children/parents
    assert_equals(
        graph.remove('node', strategy=Strategy.remove),
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
        graph.remove('bar', strategy=Strategy.remove),
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
    assert_equals(graph.remove('bravo'), frozenset(('bravo',)))

    assert_equals(
        graph.nodes,
        frozenset(
            ('alpha', 'beta')
        )
    )
    assert_equals(graph.roots, frozenset())

    # stub
    assert_equals(
        graph.remove('alpha', strategy=Strategy.remove),
        frozenset(('alpha', 'beta'))
    )

    assert_equals(
        graph.nodes,
        frozenset()
    )

    assert_raises(KeyError, graph.remove, 'fake', strategy=Strategy.remove)


def test_prune():
    """
    Prune a Graph
    """
    from arbiter.graph import Graph

    graph = Graph()

    graph.add('node')
    graph.add('bar', ('foo',))
    graph.add('baz', ('bar',))
    graph.add('beta', ('alpha',))
    graph.add('bravo', ('alpha',))

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

    graph.add('foo')

    assert_false(graph == other)
    assert_true(graph != other)

    graph.add('bar', ('foo',))

    other.add('bar', ('foo',))

    # still shouldn't match graph['foo'] is a stub
    assert_false(graph == other)
    assert_true(graph != other)

    other.add('foo')

    assert_true(graph == other)
    assert_false(graph != other)


def test_naming():
    """
    Node names just need to be hashable.
    """
    from arbiter.graph import Graph, Strategy

    graph = Graph()

    for name in (1, float('NaN'), 0, None, '', frozenset(), (), False, sum):
        graph.add('child1', frozenset((name,)))

        graph.add(name)

        assert_true(name in graph)
        assert_equals(graph.nodes, frozenset((name, 'child1')))
        assert_equals(graph.roots, frozenset((name,)))
        assert_equals(graph.children(name), frozenset(('child1',)))
        assert_equals(graph.parents(name), frozenset())
        assert_false(graph.ancestor_of(name, name))

        graph.add('child2', frozenset((name,)))
        assert_equals(
            graph.children(name),
            frozenset(('child1', 'child2'))
        )

        graph.remove(name)
        graph.remove('child1')
        graph.remove('child2')

        assert_equals(graph.nodes, frozenset())

    graph.add(None)
    graph.add('', parents=((),))
    graph.add((), parents=(frozenset(),))
    graph.add(frozenset())

    assert_equals(graph.nodes, frozenset((None, '', (), frozenset())))
    assert_equals(graph.roots, frozenset((None, frozenset())))

    graph.remove((), strategy=Strategy.remove)

    assert_equals(graph.nodes, frozenset((None, frozenset())))
    assert_equals(graph.roots, frozenset((None, frozenset())))

    assert_raises(TypeError, graph.add, [])
    assert_raises(TypeError, graph.add, 'valid', parents=([],))
