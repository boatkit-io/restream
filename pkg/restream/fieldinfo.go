package restream

import (
	"fmt"
)

// FieldInfo is a struct that contains information about a single field in a struct.
type FieldInfo struct {
	Name     string
	FieldIdx int
	FieldID  byte
	VarInfo  VarInfo
}

// ToGolangString returns a string representation of the FieldInfo in golang.
func (fi *FieldInfo) ToGolangString() string {
	if fi.FieldID != 0 {
		return fmt.Sprintf("{Name: \"%s\", FieldIdx: %d, FieldID: %d, VarInfo: &%s},\n",
			fi.Name, fi.FieldIdx, fi.FieldID, fi.VarInfo.ToGolangString())
	}

	return fmt.Sprintf("{Name: \"%s\", FieldIdx: %d, VarInfo: &%s},\n",
		fi.Name, fi.FieldIdx, fi.VarInfo.ToGolangString())
}

// ToTSString returns a string representation of the FieldInfo in typescript.
func (fi *FieldInfo) ToTSString() string {
	if fi.FieldID != 0 {
		return fmt.Sprintf("{name: \"%s\", fieldIdx: %d, fieldID: %d, varInfo: %s}",
			fi.Name, fi.FieldIdx, fi.FieldID, fi.VarInfo.ToTSString())
	}

	return fmt.Sprintf("{name: \"%s\", fieldIdx: %d, varInfo: %s}",
		fi.Name, fi.FieldIdx, fi.VarInfo.ToTSString())
}
