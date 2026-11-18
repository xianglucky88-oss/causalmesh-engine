import json
class ORSet:
    def __init__(self,replica):
        if not isinstance(replica,str) or not replica:raise ValueError("replica identity required")
        self.replica=replica;self.counter=0;self.adds={};self.removed=set()
    def add(self,value):
        if not isinstance(value,str):raise ValueError("string elements required")
        self.counter+=1;self.adds.setdefault(value,set()).add((self.replica,self.counter))
    def remove(self,value):self.removed.update(self.adds.get(value,set()))
    def values(self):return sorted(v for v,dots in self.adds.items() if dots-self.removed)
    def merge(self,other):
        if self.replica==other.replica and self is not other:raise ValueError("distinct writers must not reuse identity")
        for value,dots in other.adds.items():self.adds.setdefault(value,set()).update(dots)
        self.removed|=other.removed
        return self
    def dump(self):return json.dumps({"version":1,"replica":self.replica,"counter":self.counter,"adds":{v:sorted(ds) for v,ds in self.adds.items()},"removed":sorted(self.removed)},sort_keys=True)
    @classmethod
    def load(cls,text):
        d=json.loads(text)
        if d.get("version")!=1:raise ValueError("unsupported version")
        s=cls(d["replica"]);s.counter=d["counter"]
        if not isinstance(s.counter,int) or s.counter<0:raise ValueError("invalid counter")
        for v,ds in d["adds"].items():
            dots=set()
            for writer,n in ds:
                if not isinstance(writer,str) or not isinstance(n,int) or n<1 or (writer==s.replica and n>s.counter):raise ValueError("invalid dot")
                dots.add((writer,n))
            s.adds[v]=dots
        s.removed={tuple(x) for x in d["removed"]}
        if not s.removed<=set().union(set(),*s.adds.values()):raise ValueError("unknown removed dot")
        return s
