package packetecs2

type EntityManager struct {
	Entities   map[uint32]*Entity
	Uid_lookup map[int32]*Entity
	IDs        map[uint32]EntityID
}

func (mgr *EntityManager) AddEntity(eid EntityID, entity *Entity) {
	mgr.removeAt(eid.Index())
	if mgr.IDs == nil {
		mgr.IDs = map[uint32]EntityID{}
	}
	mgr.Entities[eid.Index()] = entity
	mgr.IDs[eid.Index()] = eid
	val, ok := GetObjectData[int32](&entity.Data, "uid")
	if ok {
		mgr.Uid_lookup[val] = entity
	}
}

func (mgr *EntityManager) RemoveEntity(eid EntityID) {
	if known, ok := mgr.IDs[eid.Index()]; ok && known != eid {
		return
	}
	mgr.removeAt(eid.Index())
}

func (mgr *EntityManager) removeAt(index uint32) {
	entity := mgr.Entities[index]
	if entity == nil {
		return
	}
	if uid, ok := GetObjectData[int32](&entity.Data, "uid"); ok && mgr.Uid_lookup[uid] == entity {
		delete(mgr.Uid_lookup, uid)
	}
	delete(mgr.Entities, index)
	delete(mgr.IDs, index)
}

func (mgr *EntityManager) GetEntity(eid EntityID) (*Entity, bool) { // TODO, maybe return an err instead?
	val, ok := mgr.Entities[eid.Index()]
	return val, ok
}

func (mgr *EntityManager) GetEntityByUid(uid int32) (*Entity, bool) {
	val, ok := mgr.Uid_lookup[uid]
	return val, ok
}
