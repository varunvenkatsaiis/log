package logs

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64( len(write)  ) + lenwidth
)

func TestStoreAppendRead( t *testing.T )    {
	f , err := os.CreateTemp("" , "store_append_read_test")
	require.NoError(t , err)
	defer os.Remove(f.Name() )

	s,err := newStore(f)
	require.NoError(t , err)

	testAppend(t , s)
	testRead(t , s)
	testReadAt(t , s)

	s , err = newStore(f)
	require.NoError(t , err)
	testRead(t , s)

}

func testAppend(t *testing.T , s *store){
	t.Helper()

	for i := uint64(1) ; i < 4 ; i++ {
		n , pos , err := s.Append(write)
		require.NoError(t , err)
		require.Equal(t , pos+n , width*i)

	}
}

func testRead(t *testing.T , s *store){
	t.Helper()
	var pos uint64
	for i := uint64(1) ; i < 4 ; i++{
		read ,err := s.Read(pos)
		require.NoError(t , err)
		require.Equal(t , write , read)
		pos = pos + width

	}
}

func testReadAt(t *testing.T , s *store){
	t.Helper()
	off := int64(0) 
	for i := uint64(1) ; i < 4 ; i++{
		b := make([]byte , lenwidth)

		n , err := s.ReadAt(b , int64(off))
		
		require.NoError(t , err)

		require.Equal(t ,lenwidth , n)

		off += int64(n) 

		size := enc.Uint64(b)

		b = make([]byte, size)

		n , err = s.ReadAt(b , off 	)

		require.NoError(t , err)
		require.Equal(t , write , b)
		require.Equal(t ,int(size) , n)
		off += int64(n)
	}
}

func TestStoreClose( t *testing.T ) {
	f , err := os.CreateTemp("" , "store_close_test")

	require.NoError(t , err)
	defer os.Remove(f.Name() )
	s , err := newStore(f)
	require.NoError(t , err)
	_, _ , err = s.Append(write)
	require.NoError(t , err)

	f , beforesize , err := OpenFile( f.Name() )
	require.NoError(t , err)

	err = s.close()
	require.NoError(t , err)
	_ , aftersize , err := OpenFile( f.Name() )
	require.NoError(t , err)

	require.True(t , aftersize > beforesize)

}

func OpenFile( name string) (file *os.File , size int64 , err error) {
	f , err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil , 0 , err
	}
	 fi , err := f.Stat()

	 if err != nil {
		return nil , 0 , err
	 }
	 return f , fi.Size() , nil
}




