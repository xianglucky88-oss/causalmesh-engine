import itertools
import pytest
from causalmesh import replicas,transactions,snapshots,placement,aggregation

def test_observed_remove_and_concurrent_add():
    a,b=replicas.ORSet('a'),replicas.ORSet('b');a.add('x');b.merge(a);a.remove('x');b.add('x');a.merge(b);b.merge(a)
    assert a.values()==b.values()==['x']
    a.remove('x');b.merge(a);assert b.values()==[]
    c=replicas.ORSet.load(a.dump());assert c.values()==a.values()
def test_merge_order_and_duplicate_delivery():
    originals=[]
    for name in 'abc':
        s=replicas.ORSet(name);s.add(name);originals.append(s)
    results=[]
    for order in itertools.permutations(originals):
        r=replicas.ORSet('reader')
        for x in order:r.merge(x);r.merge(x)
        results.append(r.values())
    assert all(x==['a','b','c'] for x in results)
def test_conflict_history_cycle_and_abort():
    h=[{'tx':'a','op':'read','key':'x'},{'tx':'b','op':'read','key':'y'},{'tx':'a','op':'write','key':'y'},{'tx':'b','op':'write','key':'x'}]
    r=transactions.analyze(h);assert not r['conflict_serializable'] and r['cycle'][0]==r['cycle'][-1]
    assert transactions.analyze(h,aborted=['b'])['conflict_serializable']
    assert transactions.analyze([{'tx':'a','op':'write','key':'x'},{'tx':'b','op':'read','key':'x'}])['order']==['a','b']
def test_real_fifo_marker_captures_in_flight():
    n=snapshots.Network(['a','b']);n.send('b','a','old');n.start('a');n.deliver('a','b');n.deliver('b','a');n.drain()
    r=n.result();assert r['a']['state']==[] and r['a']['channels']['b']==['old']
    assert n.state['a']==['old']
def test_overlapping_snapshot_sessions():
    n=snapshots.Network(['a','b','c']);n.send('a','b',1);n.start('a','one');n.start('c','two');n.drain()
    assert len(n.result('one'))==len(n.result('two'))==3
    with pytest.raises(ValueError):n.start('a','one')
def test_placement_certificate_and_impossibility():
    r=placement.place(['a','b','c'],2,[['a'],['b'],['c']]);assert r['feasible'] and placement.verify(r['placement'],['a','b','c'],[['a'],['b'],['c']])
    assert not placement.place(['a','b'],1,[['a'],['b']])['feasible']
    with pytest.raises(ValueError):placement.place(list(map(str,range(20))),10,[],max_candidates=2)
def test_masked_fixed_roster_sum_and_abort():
    clients=[aggregation.Client(x) for x in ['a','b','c']];keys={c.identity:c.public() for c in clients}
    messages=[c.mask(x,'round',keys) for c,x in zip(clients,[3,7,11])]
    assert aggregation.combine(messages,list(keys),'round')==21
    assert all(set(m)=={'identity','session','roster','masked'} for m in messages)
    with pytest.raises(ValueError):aggregation.combine(messages[:-1],list(keys),'round')
    with pytest.raises(ValueError):clients[0].mask(3,'round',keys)
    with pytest.raises(ValueError):aggregation.combine(messages,list(keys),'other')
