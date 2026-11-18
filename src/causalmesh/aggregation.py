import hashlib
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey,X25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
P=2**127-1
class Client:
    def __init__(self,identity):
        self.identity=identity;self._private=X25519PrivateKey.generate();self.used=set()
    def public(self):return self._private.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
    def mask(self,value,session,public_keys):
        if not isinstance(value,int) or not 0<=value<P:raise ValueError("value outside field")
        if not isinstance(session,str) or not session or session in self.used:raise ValueError("session already used")
        if len(public_keys)<3 or self.identity not in public_keys or public_keys[self.identity]!=self.public():raise ValueError("invalid participant set")
        roster=b"".join(len(k.encode()).to_bytes(4,"big")+k.encode()+public_keys[k] for k in sorted(public_keys))
        ctx=hashlib.sha256(session.encode()+roster).digest();result=value
        for identity,public in sorted(public_keys.items()):
            if identity==self.identity:continue
            shared=self._private.exchange(X25519PublicKey.from_public_bytes(public))
            mask=int.from_bytes(hashlib.sha256(b"aggregate-v1"+ctx+shared).digest(),"big")%P
            result=(result+mask if self.identity<identity else result-mask)%P
        self.used.add(session)
        return {"identity":self.identity,"session":session,"roster":hashlib.sha256(roster).hexdigest(),"masked":result}

def combine(messages,participants,session):
    if len(participants)<3 or len(participants)!=len(set(participants)):raise ValueError("invalid participant set")
    if len(messages)!=len(participants) or {m['identity'] for m in messages}!=set(participants):raise ValueError("all participants required; round aborted")
    if any(m["session"]!=session or not isinstance(m["masked"],int) or not 0<=m["masked"]<P for m in messages) or len({m["roster"] for m in messages})!=1:raise ValueError("round mismatch")
    return sum(m["masked"] for m in messages)%P
