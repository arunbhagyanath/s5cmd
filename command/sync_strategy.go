package command

import (
	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/storage"
)

// SyncStrategy is the interface to make decision whether given source object should be synced
// to destination object
type SyncStrategy interface {
	ShouldSync(srcObject, dstObject *storage.Object) error
}

func NewStrategy(sizeOnly bool) SyncStrategy {
	if sizeOnly {
		return &SizeOnlyStrategy{}
	} else {
		return &SizeAndModificationStrategy{}
	}
}

// EtagAwareStrategy wraps another strategy but short-circuits when both objects
// have matching non-empty etags (meaning content is identical). This is useful
// for cache-based sync where etags are preserved.
type EtagAwareStrategy struct {
	Inner SyncStrategy
}

func (e *EtagAwareStrategy) ShouldSync(srcObj, dstObj *storage.Object) error {
	if srcObj.Etag != "" && srcObj.Etag == dstObj.Etag {
		return errorpkg.ErrObjectSizesMatch // content identical, skip
	}
	return e.Inner.ShouldSync(srcObj, dstObj)
}

// SizeOnlyStrategy determines to sync based on objects' file sizes.
type SizeOnlyStrategy struct{}

func (s *SizeOnlyStrategy) ShouldSync(srcObj, dstObj *storage.Object) error {
	if srcObj.Size == dstObj.Size {
		return errorpkg.ErrObjectSizesMatch
	}
	return nil
}

// SizeAndModificationStrategy determines to sync based on objects' both sizes and modification times.
// It treats source object as the source-of-truth;
//
//	time: src > dst        size: src != dst    should sync: yes
//	time: src > dst        size: src == dst    should sync: yes
//	time: src <= dst       size: src != dst    should sync: yes
//	time: src <= dst       size: src == dst    should sync: no
type SizeAndModificationStrategy struct{}

func (sm *SizeAndModificationStrategy) ShouldSync(srcObj, dstObj *storage.Object) error {
	srcMod, dstMod := srcObj.ModTime, dstObj.ModTime
	if srcMod.After(*dstMod) {
		return nil
	}

	if srcObj.Size != dstObj.Size {
		return nil
	}

	return errorpkg.ErrObjectIsNewerAndSizesMatch
}
