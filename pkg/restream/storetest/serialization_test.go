package storetest

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/stretchr/testify/assert"
)

type BTRet struct {
	name    string
	bytes   []byte
	jsonStr string
}

type basicTesterFixtureRow struct {
	Name    string `json:"name"`
	Bytes   []int  `json:"bytes"`
	JSONStr string `json:"jsonStr"`
}

type TestString string

func BasicTester[T any, TDO any](t *testing.T, iv T) BTRet {
	vi, err := restream.VarInfoFromType(reflect.TypeOf(iv))
	assert.NoError(t, err)

	// Make up a name for the test for the TS test generation
	var nt string
	ivt := reflect.ValueOf(iv)
	if ivt.Kind() == reflect.Pointer && !ivt.IsNil() {
		nt = fmt.Sprintf("%+v", ivt.Elem().Interface())
	} else {
		nt = fmt.Sprintf("%+v", iv)
	}
	ret := BTRet{
		name: fmt.Sprintf("[%s,%s] %s", reflect.TypeFor[T]().String(), reflect.TypeFor[TDO]().String(), nt),
	}

	// No pointers either side
	w, b := binarystreams.NewMemoryWriter()
	assert.NoError(t, err)
	assert.NoError(t, restream.SerializeValue(iv, w, vi))
	assert.NoError(t, w.Flush())

	r := binarystreams.NewReaderFromBytes(b.Bytes())
	var ov T
	assert.NoError(t, restream.DeserializeValue(&ov, r, vi))
	assert.True(t, deepComparableEqual(iv, ov))

	// Pointer both
	ivp := &iv
	vip := &restream.VarInfoPointer{SubType: vi}
	w4, b4 := binarystreams.NewMemoryWriter()
	assert.NoError(t, restream.SerializeValue(ivp, w4, vip))
	assert.NoError(t, w4.Flush())

	r4 := binarystreams.NewReaderFromBytes(b4.Bytes())
	var ov4 *T
	assert.NoError(t, restream.DeserializeValue(&ov4, r4, vip))
	assert.True(t, deepComparableEqual(*ivp, *ov4))

	if reflect.TypeFor[TDO]().Kind() != reflect.Interface {
		// Try dynamic deserialization of the VI type
		bs, err := vi.GetSerializationData()
		assert.NoError(t, err)
		vi2, err := restream.VarInfoFromReader(binarystreams.NewReaderFromBytes(bs))
		assert.NoError(t, err)
		assert.True(t, deepComparableEqual(vi, vi2))

		// Let's try adding dynamic types
		vid := &restream.VarInfoDynamic{}
		wd, bd := binarystreams.NewMemoryWriter()
		ivd := any(iv)
		assert.NoError(t, restream.SerializeDynamicValue(ivd, wd, vid))
		assert.NoError(t, wd.Flush())

		rd := binarystreams.NewReaderFromBytes(bd.Bytes())
		var ovd any
		assert.NoError(t, restream.DeserializeDynamicValue(&ovd, rd, vid))
		assert.True(t, deepComparableEqual(any(iv), ovd))

		ret.bytes = bd.Bytes()
		ob, err := json.Marshal(ovd)
		assert.NoError(t, err)
		ret.jsonStr = string(ob)

		if reflect.TypeFor[T]().Kind() != reflect.Pointer {
			// And again, with pointers -- don't do double pointers
			vip := &restream.VarInfoPointer{SubType: vi}
			wd2, bd2 := binarystreams.NewMemoryWriter()
			ivd2 := any(&iv)
			assert.NoError(t, restream.SerializeValue(ivd2, wd2, vip))
			assert.NoError(t, wd2.Flush())

			rd2 := binarystreams.NewReaderFromBytes(bd2.Bytes())
			var ovd2 any
			assert.NoError(t, restream.DeserializeValue(&ovd2, rd2, vip))
			assert.True(t, deepComparableEqual(ivd2, ovd2))
		}
	}

	return ret
}

func TestBasicTypes(t *testing.T) {
	testRets := []BTRet{
		BasicTester[bool, bool](t, false),
		BasicTester[bool, bool](t, true),

		BasicTester[uint8, uint8](t, 0),
		BasicTester[uint8, uint8](t, 37),
		BasicTester[uint8, uint8](t, ^uint8(0)),
		BasicTester[*uint8, *uint8](t, nil),
		BasicTester[*uint8, *uint8](t, restream.Ptr(uint8(4))),
		BasicTester[uint16, uint16](t, 0),
		BasicTester[uint16, uint16](t, 27),
		BasicTester[uint16, uint16](t, ^uint16(0)),
		BasicTester[uint32, uint32](t, 0),
		BasicTester[uint32, uint32](t, 27),
		BasicTester[uint32, uint32](t, 65535),
		BasicTester[uint32, uint32](t, ^uint32(0)),
		BasicTester[uint64, uint64](t, 0),
		BasicTester[uint64, uint64](t, 27),
		BasicTester[uint64, uint64](t, 65535),
		BasicTester[uint64, uint64](t, uint64(^uint32(0))),
		BasicTester[uint64, uint64](t, ^uint64(0)),
		BasicTester[uint, uint64](t, 0),
		BasicTester[uint, uint64](t, 27),
		BasicTester[uint, uint64](t, 65535),
		BasicTester[uint, uint64](t, ^uint(0)),

		BasicTester[int8, int8](t, 0),
		BasicTester[int8, int8](t, 37),
		BasicTester[int8, int8](t, -50),
		BasicTester[int8, int8](t, ^int8(0)),
		BasicTester[int16, int16](t, 0),
		BasicTester[int16, int16](t, 27),
		BasicTester[int16, int16](t, -50),
		BasicTester[int16, int16](t, ^int16(0)),
		BasicTester[int32, int32](t, 0),
		BasicTester[int32, int32](t, 27),
		BasicTester[int32, int32](t, -50),
		BasicTester[int32, int32](t, 65535),
		BasicTester[int32, int32](t, ^int32(0)),
		BasicTester[int64, int64](t, 0),
		BasicTester[int64, int64](t, 27),
		BasicTester[int64, int64](t, -50),
		BasicTester[int64, int64](t, 65535),
		BasicTester[int64, int64](t, int64(^uint32(0))),
		BasicTester[int64, int64](t, ^int64(0)),
		BasicTester[int, int64](t, 0),
		BasicTester[int, int64](t, 27),
		BasicTester[int, int64](t, -50),
		BasicTester[int, int64](t, 65535),
		BasicTester[int, int64](t, ^int(0)),

		BasicTester[string, string](t, ""),
		BasicTester[string, string](t, "testme"),
		BasicTester[*string, *string](t, nil),
		BasicTester[*string, *string](t, restream.Ptr("test")),
		BasicTester[TestString, TestString](t, TestString("")),
		BasicTester[TestString, TestString](t, TestString("testme")),

		BasicTester[float32, float32](t, 0),
		BasicTester[float32, float32](t, -1),
		BasicTester[float32, float32](t, 1000),
		BasicTester[float32, float32](t, -1.43534123),
		BasicTester[float32, float32](t, 14.3534123),

		BasicTester[float64, float64](t, 0),
		BasicTester[float64, float64](t, -1),
		BasicTester[float64, float64](t, 1000),
		BasicTester[float64, float64](t, -1.43534),
		BasicTester[float64, float64](t, 14.3534),

		BasicTester[time.Time, time.Time](t, time.Date(2024, 3, 5, 10, 5, 3, 16000000, time.UTC)),

		BasicTester[[]int8, []int8](t, []int8{-4, -3, -2, -1, 0, 1, 2, 3, 4}),
		BasicTester[[]int16, []int16](t, []int16{-4, -3, -2, -1, 0, 1, 2, 3, 4}),
		BasicTester[[]int32, []int32](t, []int32{-4, -3, -2, -1, 0, 1, 2, 3, 4}),
		BasicTester[[]int64, []int64](t, []int64{-4, -3, -2, -1, 0, 1, 2, 3, 4}),
		BasicTester[[]int, []int64](t, []int{-4, -3, -2, -1, 0, 1, 2, 3, 4}),
		BasicTester[[]byte, []byte](t, []byte{0, 1, 2, 3, 4}),
		BasicTester[[]byte, []byte](t, nil),
		BasicTester[[]uint8, []uint8](t, []uint8{0, 1, 2, 3, 4}),
		BasicTester[[]uint16, []uint16](t, []uint16{0, 1, 2, 3, 4}),
		BasicTester[[]uint32, []uint32](t, []uint32{0, 1, 2, 3, 4}),
		BasicTester[[]uint64, []uint64](t, []uint64{0, 1, 2, 3, 4}),
		BasicTester[[]uint, []uint64](t, []uint{0, 1, 2, 3, 4}),
		BasicTester[[]any, []any](t, []any{uint32(0), int16(-4), uint64(7000), "hello", false, 3.7, float64(3.3)}),

		BasicTester[map[int64]struct{}, any](t, nil),
		BasicTester[map[int64]string, map[int64]string](t, map[int64]string{0: "hi", 4: "blah", -1: ""}),
		BasicTester[map[int32]int8, map[int32]int8](t, map[int32]int8{0: 4, 5: 7, 9: -4, -5: 0}),
		BasicTester[map[int32]int8, map[int32]int8](t, nil),
		BasicTester[map[string]map[string]int, map[string]map[string]int64](t,
			map[string]map[string]int{"hi": {"hello": 1, "world": 2}, "foo": {"bar": 3}}),

		BasicTester[[]any, []any](t, []any{map[string]map[string][]int{"hi": {"hello": []int{1, 2, 3},
			"world": []int{2}}, "foo": {"bar": []int{3}}}}),

		BasicTester[restream.PartialArray[int], any](t, *restream.NewPartialArray[int]().Set(0, 4).Set(1, 5).SetWhole([]int{3, 4, 5, 6})),

		BasicTester[restream.PartialValue[TestState, *TestStatePartial], any](t, *(&restream.PartialValue[TestState, *TestStatePartial]{}).
			SetWhole(&TestState{BaseStruct: TestMapData{Number: 4}}).ApplyPartial(&TestStatePartial{BaseField: restream.Ptr("hello")})),
	}

	fixturePath := writeBasicTesterFixture(t, testRets)
	runBasicTesterSpec(t, fixturePath)
}

func writeBasicTesterFixture(t *testing.T, testRets []BTRet) string {
	t.Helper()

	rows := make([]basicTesterFixtureRow, 0, len(testRets))
	for _, tr := range testRets {
		if len(tr.bytes) == 0 {
			continue
		}
		rows = append(rows, basicTesterFixtureRow{
			Name:    tr.name,
			Bytes:   bytesToInts(tr.bytes),
			JSONStr: tr.jsonStr,
		})
	}

	payload, err := json.Marshal(rows)
	assert.NoError(t, err)

	fixturePath := filepath.Join(t.TempDir(), "BasicTesterData.json")
	assert.NoError(t, os.WriteFile(fixturePath, payload, 0o644))
	return fixturePath
}

func bytesToInts(bs []byte) []int {
	ret := make([]int, len(bs))
	for i, b := range bs {
		ret[i] = int(b)
	}
	return ret
}

func runBasicTesterSpec(t *testing.T, fixturePath string) {
	t.Helper()

	cmd := exec.Command("pnpm", "exec", "vitest", "run", "src/restream/BasicTester.spec.ts")
	cmd.Dir = "../../../web"
	cmd.Env = append(os.Environ(), "RESTREAM_BASIC_TESTER_DATA="+fixturePath)

	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(out))
}

func deepComparableEqual(a, b any) bool {
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	switch av.Kind() {
	case reflect.Slice:
		if av.Kind() != bv.Kind() {
			return false
		}
		if av.Len() != bv.Len() {
			return false
		}
		for i := 0; i < av.Len(); i++ {
			if !deepComparableEqual(av.Index(i).Interface(), bv.Index(i).Interface()) {
				return false
			}
		}
		return true
	case reflect.Map:
		if av.Kind() != bv.Kind() {
			return false
		}
		if av.Len() != bv.Len() {
			return false
		}
		for _, k := range av.MapKeys() {
			avVal := av.MapIndex(k)
			bvVal := bv.MapIndex(k)
			if !avVal.IsValid() || !bvVal.IsValid() || !deepComparableEqual(avVal.Interface(), bvVal.Interface()) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if av.Kind() != bv.Kind() {
			return false
		}
		if av.Type() != bv.Type() {
			return false
		}
		vf := reflect.VisibleFields(av.Type())
		for _, f := range vf {
			if !f.IsExported() {
				continue
			}
			if !deepComparableEqual(av.FieldByIndex(f.Index).Interface(), bv.FieldByIndex(f.Index).Interface()) {
				return false
			}
		}
		return true
	case reflect.Pointer:
		if av.Pointer() == bv.Pointer() {
			return true
		}
		return deepComparableEqual(av.Elem().Interface(), bv.Elem().Interface())
	case reflect.Interface:
		return deepComparableEqual(av.Elem().Interface(), bv.Elem().Interface())
	default:
		if av.Equal(bv) {
			return true
		}
		if av.CanConvert(bv.Type()) {
			return av.Convert(bv.Type()).Equal(bv)
		}
		return false
	}
}

func TestPackers(t *testing.T) {
	// -1 gets the iv = 0 case
	for i := -1; i < 64; i++ {
		iv := uint64(0)
		for h := 0; h <= i; h++ {
			iv |= 1 << h
		}

		w, b := binarystreams.NewMemoryWriter()
		assert.NoError(t, restream.SerializePacked64(iv, w))
		assert.NoError(t, w.Flush())

		switch {
		case i < 6:
			assert.Equal(t, 1, b.Len())
		case i < 13:
			assert.Equal(t, 2, b.Len())
		case i < 28:
			assert.Equal(t, 4, b.Len())
		case i < 59:
			assert.Equal(t, 8, b.Len())
		default:
			assert.Equal(t, 9, b.Len())
		}

		r := binarystreams.NewReaderFromBytes(b.Bytes())
		ov, err := restream.DeserializePacked64[uint64](r)
		assert.NoError(t, err)
		assert.Equal(t, iv, ov)
	}

	// -1 gets the iv = 0 case
	for i := -1; i < 63; i++ {
		iv := int64(0)
		for h := 0; h <= i; h++ {
			iv |= 1 << h
		}

		w, b := binarystreams.NewMemoryWriter()
		assert.NoError(t, restream.SerializePacked64(iv, w))
		assert.NoError(t, w.Flush())

		switch {
		case i < 6:
			assert.Equal(t, 1, b.Len())
		case i < 13:
			assert.Equal(t, 2, b.Len())
		case i < 28:
			assert.Equal(t, 4, b.Len())
		case i < 59:
			assert.Equal(t, 8, b.Len())
		default:
			assert.Equal(t, 9, b.Len())
		}

		r := binarystreams.NewReaderFromBytes(b.Bytes())
		ov, err := restream.DeserializePacked64[int64](r)
		assert.NoError(t, err)
		assert.Equal(t, iv, ov)

		// try again with negative
		iv = -iv

		w, b = binarystreams.NewMemoryWriter()
		assert.NoError(t, restream.SerializePacked64(iv, w))
		assert.NoError(t, w.Flush())

		switch {
		case i < 6:
			assert.Equal(t, 1, b.Len())
		case i < 13:
			assert.Equal(t, 2, b.Len())
		case i < 28:
			assert.Equal(t, 4, b.Len())
		case i < 59:
			assert.Equal(t, 8, b.Len())
		default:
			assert.Equal(t, 9, b.Len())
		}

		r = binarystreams.NewReaderFromBytes(b.Bytes())
		ov, err = restream.DeserializePacked64[int64](r)
		assert.NoError(t, err)
		assert.Equal(t, iv, ov)
	}
}

func TestVarOptionsArray(t *testing.T) {
	iv := []int{1, 2, 3}
	var ov []int

	vi, err := restream.VarInfoFromType(reflect.TypeOf(iv))
	assert.NoError(t, err)

	b, err := restream.SerializeValueToBytes(iv, vi)
	assert.NoError(t, err)

	r := binarystreams.NewReaderFromBytes(b)
	assert.NoError(t, restream.DeserializeValue(&ov, r, vi))
	assert.Equal(t, iv, ov)

	vi.(*restream.VarInfoArray).NotNil = true
	b2, err := restream.SerializeValueToBytes(iv, vi)
	assert.NoError(t, err)

	r2 := binarystreams.NewReaderFromBytes(b2)
	assert.NoError(t, restream.DeserializeValue(&ov, r2, vi))
	assert.Equal(t, iv, ov)
}

func TestVarOptionsMap(t *testing.T) {
	iv := map[int]*int{1: restream.Ptr(1), 2: restream.Ptr(2), 3: restream.Ptr(3)}
	var ov map[int]*int

	vi, err := restream.VarInfoFromType(reflect.TypeOf(iv))
	assert.NoError(t, err)

	b, err := restream.SerializeValueToBytes(iv, vi)
	assert.NoError(t, err)

	r := binarystreams.NewReaderFromBytes(b)
	assert.NoError(t, restream.DeserializeValue(&ov, r, vi))
	assert.Equal(t, iv, ov)

	vi.(*restream.VarInfoMap).NotNil = true
	b2, err := restream.SerializeValueToBytes(iv, vi)
	assert.NoError(t, err)

	r2 := binarystreams.NewReaderFromBytes(b2)
	assert.NoError(t, restream.DeserializeValue(&ov, r2, vi))
	assert.Equal(t, iv, ov)

	vi.(*restream.VarInfoMap).ElemType.(*restream.VarInfoPointer).NotNil = true
	b3, err := restream.SerializeValueToBytes(iv, vi)
	assert.NoError(t, err)

	r3 := binarystreams.NewReaderFromBytes(b3)
	assert.NoError(t, restream.DeserializeValue(&ov, r3, vi))
	assert.Equal(t, iv, ov)
}
