import itertools,math

def place(resources,replicas,failure_sets,max_candidates=100000):
    if len(resources)!=len(set(resources)) or not resources or len(resources)>30:raise ValueError("invalid resource identities")
    if not isinstance(replicas,int) or not 1<=replicas<=len(resources):raise ValueError("invalid replica count")
    if math.comb(len(resources),replicas)>max_candidates:raise ValueError("search budget exceeded")
    allowed=set(resources);failures=[set(f) for f in failure_sets]
    if any(not f<=allowed for f in failures):raise ValueError("unknown failure resource")
    for choice in itertools.combinations(sorted(resources),replicas):
        c=set(choice)
        if all(c-f for f in failures):return {"feasible":True,"placement":list(choice),"survivors":[sorted(c-f) for f in failures]}
    return {"feasible":False,"placement":[],"checked":math.comb(len(resources),replicas)}

def verify(placement,resources,failure_sets):
    c=set(placement)
    return len(c)==len(placement) and c<=set(resources) and all(c-set(f) for f in failure_sets)
