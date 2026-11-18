from collections import deque
class Network:
    def __init__(self,nodes):
        if len(nodes)!=len(set(nodes)) or not nodes or len(nodes)>16:raise ValueError("invalid process set")
        self.nodes=tuple(nodes);self.state={n:[] for n in nodes}
        self.links={(a,b):deque() for a in nodes for b in nodes if a!=b};self.snap={};self.work=0
    def send(self,a,b,value):
        if (a,b) not in self.links:raise ValueError("unknown channel")
        self.links[a,b].append(("data",value))
    def start(self,node,session="s"):
        if session in self.snap:raise ValueError("duplicate capture session")
        if node not in self.nodes:raise ValueError("unknown process")
        self.snap[session]={};self._record(node,session)
    def _record(self,node,session):
        self.snap[session][node]={"state":list(self.state[node]),"open":{a for a in self.nodes if a!=node},"channels":{a:[] for a in self.nodes if a!=node}}
        for other in self.nodes:
            if other!=node:self.links[node,other].append(("marker",session))
    def deliver(self,a,b):
        q=self.links[a,b]
        if not q:return False
        self.work+=1
        if self.work>100000:raise ValueError("message limit")
        kind,value=q.popleft()
        if kind=="marker":
            if b not in self.snap[value]:self._record(b,value)
            self.snap[value][b]["open"].discard(a)
        else:
            for session,nodes in self.snap.items():
                if b in nodes and a in nodes[b]["open"]:nodes[b]["channels"][a].append(value)
            self.state[b].append(value)
        return True
    def drain(self):
        while any(self.links.values()):
            for a,b in self.links:self.deliver(a,b)
    def result(self,session="s"):
        nodes=self.snap[session]
        if len(nodes)!=len(self.nodes) or any(s["open"] for s in nodes.values()):raise ValueError("capture incomplete")
        return {n:{"state":r["state"],"channels":r["channels"]} for n,r in nodes.items()}
