package restream

// SerializationType is an enum for all of the dynamic serialization types
// ENUM(Invalid,Bool,String,Int8,Int16,Int32,Int64,Uint8,Uint16,Uint32,Uint64,Float32,Float64,Time,Pointer,Array,Map,Dynamic,Void)
//
//go:generate go run github.com/abice/go-enum@latest
type SerializationType byte
