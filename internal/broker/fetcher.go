package broker

// fetcherManager runs follower fetchers. Phase 1 (single node) has no
// followers; replication is added in Phase 3.
type fetcherManager struct{ rm *ReplicaManager }

func newFetcherManager(rm *ReplicaManager) *fetcherManager { return &fetcherManager{rm: rm} }

func (f *fetcherManager) add(p *Partition, leader int32, epoch int32) {}
func (f *fetcherManager) remove(p *Partition)                         {}
func (f *fetcherManager) closeAll()                                   {}

func (rm *ReplicaManager) checkISR() {}
