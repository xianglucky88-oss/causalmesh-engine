import heapq

def analyze(history,aborted=()):
    if len(history)>1000:raise ValueError("history limit")
    aborted=set(aborted);ops=[];nodes=set()
    for row in history:
        tx,kind,key=row["tx"],row["op"],row["key"]
        if not all(isinstance(x,str) and x for x in [tx,kind,key]) or kind not in {"read","write"}:raise ValueError("invalid history operation")
        if tx not in aborted:ops.append((tx,kind,key));nodes.add(tx)
    if len(nodes)>128:raise ValueError("transaction count limit")
    graph={n:set() for n in nodes}
    for i,(a,op,key) in enumerate(ops):
        for b,other,k in ops[i+1:]:
            if a!=b and key==k and (op=="write" or other=="write"):graph[a].add(b)
    indegree={n:0 for n in nodes}
    for edges in graph.values():
        for n in edges:indegree[n]+=1
    ready=[n for n in nodes if not indegree[n]];heapq.heapify(ready);order=[]
    while ready:
        n=heapq.heappop(ready);order.append(n)
        for nxt in sorted(graph[n]):
            indegree[nxt]-=1
            if not indegree[nxt]:heapq.heappush(ready,nxt)
    if len(order)==len(nodes):return {"conflict_serializable":True,"order":order,"cycle":[]}
    color={};path=[];found=[]
    def dfs(n):
        color[n]=1;path.append(n)
        for v in sorted(graph[n]):
            if color.get(v)==1:found.extend(path[path.index(v):]+[v]);return True
            if not color.get(v) and dfs(v):return True
        path.pop();color[n]=2;return False
    for n in sorted(nodes):
        if not color.get(n) and dfs(n):break
    return {"conflict_serializable":False,"order":[],"cycle":found}
