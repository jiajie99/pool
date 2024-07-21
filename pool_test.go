package pool

import (
	"errors"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_InitPool(t *testing.T) {
	t.Run("default worker num", func(t *testing.T) {
		pool := InitPool(0, 0, 0)
		cnt := 0
		pool.Workers.Range(func(k, v any) bool {
			cnt++
			return true
		})
		require.Equal(t, runtime.NumCPU(), cnt)
	})
	t.Run("custom worker num", func(t *testing.T) {
		pool := InitPool(2, 2, 0)
		cnt := 0
		pool.Workers.Range(func(k, v any) bool {
			cnt++
			return true
		})
		require.Equal(t, 2, cnt)
	})
	t.Run("execute task panic", func(t *testing.T) {
		pool := InitPool(2, 2, 0)
		pool.TaskChan <- &Task{
			Args: nil,
			Fn: func(args any) (any, error) {
				panic("test")
			},
		}
		require.Equal(t, &TaskResult{
			Error:  errors.New("test"),
			Result: nil,
		}, <-pool.TaskResultChan)
		time.Sleep(25 * time.Millisecond)
		cnt := 0
		pool.Workers.Range(func(_, v any) bool {
			if !(v.(*Worker).Status == WorkerStatusFree) {
				return false
			}
			cnt++
			return true
		})
		require.Equal(t, 2, cnt)
	})
	t.Run("should increase worker", func(t *testing.T) {
		pool := InitPool(1, 5, 0)
		for i := 0; i < 5; i++ {
			pool.TaskChan <- &Task{
				ID:   strconv.Itoa(i),
				Args: nil,
				Fn: func(args any) (any, error) {
					time.Sleep(10 * time.Minute)
					return nil, nil
				},
			}
		}
		for {
			if len(pool.TaskChan) == 0 {
				break
			}
		}
		require.Equal(t, 5, SyncMapLen(pool.Workers))
	})
	t.Run("should recall worker", func(t *testing.T) {
		pool := InitPool(1, 5, 2*time.Second)
		pool.TaskChan <- &Task{
			ID:   "0",
			Args: nil,
			Fn: func(args any) (any, error) {
				time.Sleep(10 * time.Minute)
				return nil, nil
			},
		}
		for i := 1; i < 5; i++ {
			pool.TaskChan <- &Task{
				ID:   strconv.Itoa(i),
				Args: nil,
				Fn: func(args any) (any, error) {
					return nil, nil
				},
			}
		}
		cnt := 0
		go func() {
			for result := range pool.TaskResultChan {
				_ = result
				cnt++
			}
		}()
		for {
			if len(pool.TaskChan) == 0 && cnt == 4 {
				break
			}
		}
		time.Sleep(3 * time.Second)
		require.Equal(t, 1, SyncMapLen(pool.Workers))
	})
	t.Run("shutdown after increase worker", func(t *testing.T) {
		pool := InitPool(1, 5, 0)
		for i := 0; i < 5; i++ {
			pool.TaskChan <- &Task{
				ID:   strconv.Itoa(i),
				Args: "nil",
				Fn: func(args any) (any, error) {
					time.Sleep(1 * time.Second)
					return nil, nil
				},
			}
		}
		cnt := 0
		go func() {
			for result := range pool.TaskResultChan {
				_ = result
				cnt++
			}
		}()
		for {
			if len(pool.TaskChan) == 0 && cnt == 5 {
				break
			}
		}
		require.Equal(t, 5, SyncMapLen(pool.Workers))
		time.Sleep(25 * time.Millisecond)
		err := pool.ClosePool()
		require.NoError(t, err)
	})
	t.Run("receive result", func(t *testing.T) {
		pool := InitPool(2, 2, 0)
		pool.TaskChan <- &Task{
			ID:   "0",
			Args: []int{1, 2},
			Fn: func(args any) (any, error) {
				arr := args.([]int)
				return arr[0] + arr[1], nil
			},
		}
		pool.TaskChan <- &Task{
			ID:   "1",
			Args: nil,
			Fn: func(args any) (any, error) {
				panic("boom!")
			},
		}
		m := make(map[string]*TaskResult, 2)
		cnt := 0
		go func() {
			for result := range pool.TaskResultChan {
				m[result.ID] = result
				cnt++
			}
		}()
		for {
			if cnt == 2 {
				break
			}
		}
		require.Equal(t, 3, m["0"].Result)
		require.Equal(t, errors.New("boom!"), m["1"].Error)
	})
}

func TestPool_ClosePool(t *testing.T) {
	t.Run("workers are busy", func(t *testing.T) {
		pool := InitPool(2, 2, 0)
		pool.TaskChan <- &Task{
			ID:   "0",
			Args: nil,
			Fn: func(args any) (any, error) {
				time.Sleep(10 * time.Minute)
				return nil, nil
			},
		}
		time.Sleep(25 * time.Millisecond)
		for {
			if len(pool.TaskChan) == 0 {
				break
			}
		}
		err := pool.ClosePool()
		require.Equal(t, errors.New("[pool] workers are busy, waiting for them to finish"), err)
	})
	t.Run("workers are free", func(t *testing.T) {
		pool := InitPool(2, 2, 0)
		err := pool.ClosePool()
		require.NoError(t, err)
	})
}
