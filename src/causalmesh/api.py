from . import replicas,transactions,snapshots,placement,aggregation

def replica_call(d):
    a,b=replicas.ORSet('a'),replicas.ORSet('b')
    for x in d.get('a',[]):a.add(x)
    for x in d.get('b',[]):b.add(x)
    a.merge(b)
    for x in d.get('remove',[]):a.remove(x)
    b.merge(a);return {'a':a.values(),'b':b.values()}
def snapshot_call(d):
    net=snapshots.Network(d.get('nodes',['a','b']))
    for a,b,value in d.get('messages',[]):net.send(a,b,value)
    net.start(net.nodes[0]);net.drain();return net.result()
OPERATIONS={
 'replicas':replica_call,'transactions':lambda d:transactions.analyze(d['history'],d.get('aborted',[])),
 'snapshots':snapshot_call,'placement':lambda d:placement.place(d['resources'],d['replicas'],d['failure_sets']),
 'aggregation':lambda d:{'aggregate':aggregation.combine(d['messages'],d['participants'],d['session'])},
}
def aggregate_example():
    clients=[aggregation.Client(i) for i in ['a','b','c']];keys={c.identity:c.public() for c in clients}
    messages=[c.mask(v,'example-round',keys) for c,v in zip(clients,[3,5,7])]
    return {'messages':messages,'participants':['a','b','c'],'session':'example-round'}
EXAMPLES={
 'replicas':{'a':['oak'],'b':['elm'],'remove':['oak']},
 'transactions':{'history':[{'tx':'a','op':'read','key':'x'},{'tx':'b','op':'read','key':'y'},{'tx':'a','op':'write','key':'y'},{'tx':'b','op':'write','key':'x'}]},
 'snapshots':{'nodes':['a','b'],'messages':[['b','a','in-flight']]},
 'placement':{'resources':['rack-a','rack-b','rack-c'],'replicas':2,'failure_sets':[['rack-a'],['rack-b'],['rack-c']]},
}
def examples():return dict(EXAMPLES,aggregation=aggregate_example())
