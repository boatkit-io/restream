package restream

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// @restream.serializers
type LatLong struct {
	Lat  float64
	Long float64
}

const AccessLevelViewer AccessLevel = 1
const AccessLevelAdmin AccessLevel = 2

func TestCalls(t *testing.T) {
	rpcd := NewRPCDispatcher(logrus.StandardLogger())

	var calledWith *int
	rpcd.RegisterRPCHandler("call", 1, func(test int) (int, error) {
		calledWith = &test
		return 3, nil
	}, reflect.TypeFor[callRequest](), reflect.TypeFor[callResponse]())
	cr := &callRequest{Test: 4}
	b, err := SerializeToBytes(cr, nil)
	assert.NoError(t, err)
	resb, handled, err := rpcd.FireRPC("call", AccessLevelViewer, b)
	assert.True(t, handled)
	assert.NoError(t, err)

	res := callResponse{}
	err = res.Deserialize(binarystreams.NewReaderFromBytes(resb), nil)
	assert.NoError(t, err)

	assert.NotNil(t, calledWith)
	assert.Equal(t, 4, *calledWith)
	assert.Equal(t, 3, res.Result)
	assert.Nil(t, res.Error)

	var calledWith2 *LatLong
	rpcd.RegisterRPCHandler("call2", AccessLevelViewer, func(test LatLong) (*int, error) {
		calledWith2 = &test
		return nil, nil
	}, reflect.TypeFor[call2Request](), reflect.TypeFor[call2Response]())
	cr2 := &call2Request{Test: LatLong{Lat: 4, Long: 5}}
	b2, err := SerializeToBytes(cr2, nil)
	assert.NoError(t, err)
	resb, handled, err = rpcd.FireRPC("call2", AccessLevelViewer, b2)
	assert.True(t, handled)
	assert.NoError(t, err)

	res2 := call2Response{}
	err = res2.Deserialize(binarystreams.NewReaderFromBytes(resb), nil)
	assert.NoError(t, err)

	assert.NotNil(t, calledWith2)
	assert.Equal(t, LatLong{Lat: 4, Long: 5}, *calledWith2)
	assert.Nil(t, res2.Result)
	assert.Nil(t, res2.Error)

	var calledWith3 *[]int
	rpcd.RegisterRPCHandler("call3", AccessLevelAdmin, func(test []int) (*int, error) {
		calledWith3 = &test
		return nil, nil
	}, reflect.TypeFor[call3Request](), reflect.TypeFor[call3Response]())
	cr3 := &call3Request{Test: []int{4, 5, 6, 7}}
	b3, err := SerializeToBytes(cr3, nil)
	assert.NoError(t, err)
	resb, handled, err = rpcd.FireRPC("call3", AccessLevelAdmin, b3)
	assert.True(t, handled)
	assert.NoError(t, err)

	res3 := call3Response{}
	err = res3.Deserialize(binarystreams.NewReaderFromBytes(resb), nil)
	assert.NoError(t, err)

	assert.NotNil(t, calledWith3)
	assert.Equal(t, []int{4, 5, 6, 7}, *calledWith3)
	assert.Nil(t, res3.Result)
	assert.Nil(t, res3.Error)

	resb, handled, err = rpcd.FireRPC("call3", AccessLevelViewer, b3)
	assert.True(t, handled)
	assert.Error(t, err)
	assert.Nil(t, resb)
}
