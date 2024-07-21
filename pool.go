package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

const (
	WorkerStatusFree        = "free"
	WorkerStatusBusy        = "busy"
	WorkerStatusUnavailable = "unavailable"
)

type Worker struct {
	Ctx            context.Context
	Status         string
	ID             string
	Error          error
	TaskChan       chan *Task
	TaskResultChan chan *TaskResult
	ErrorChan      chan *Worker
	WaitGroup      *sync.WaitGroup
	IdleTime       time.Duration
}

func (w *Worker) Start() {
	log.Printf("[worker %s] start\n", w.ID)
	taskID := ""
	t := time.NewTicker(w.IdleTime)
	defer func() {
		if err := recover(); err != nil {
			w.Status = WorkerStatusUnavailable
			w.Error = fmt.Errorf("%s", err)
			w.TaskResultChan <- &TaskResult{
				ID:     taskID,
				Result: nil,
				Error:  w.Error,
			}
			w.ErrorChan <- w
		}
	}()
	for {
		select {
		case <-t.C:
			if w.Status != WorkerStatusFree {
				continue
			}
			w.ErrorChan <- w
			w.WaitGroup.Done()
			return
		case task := <-w.TaskChan:
			if task == nil {
				continue
			}
			log.Printf("[worker %s] receive task %s\n", w.ID, task.ID)
			w.Status = WorkerStatusBusy
			taskID = task.ID
			startAt := time.Now()
			res, err := task.Fn(task.Args)
			w.TaskResultChan <- &TaskResult{
				ID:     task.ID,
				Result: res,
				Error:  err,
			}
			log.Printf("[worker %s] finish task %s, cost: %v\n", w.ID, task.ID, time.Since(startAt))
			w.Status = WorkerStatusFree
			t.Reset(w.IdleTime)
		case <-w.Ctx.Done():
			log.Printf("[worker %s] shutdown\n", w.ID)
			w.WaitGroup.Done()
			return
		}
	}
}

type TaskResult struct {
	ID     string
	Result any
	Error  error
}

type Task struct {
	ID   string
	Args any
	Fn   func(args any) (any, error)
}

type Pool struct {
	TaskChan       chan *Task
	TaskResultChan chan *TaskResult
	ShutDown       context.CancelFunc
	Workers        *sync.Map
}

type dependencies struct {
	WorkerNum      int
	MaxWorkerNum   int
	WorkerCtx      context.Context
	TaskChan       chan *Task
	TaskResultChan chan *TaskResult
	ErrorChan      chan *Worker
	WaitGroup      *sync.WaitGroup
	WorkersMap     *sync.Map
	WorkerIdleTime time.Duration
}

func initWorkers(p dependencies) *sync.Map {
	workers := &sync.Map{}
	for i := 0; i < p.WorkerNum; i++ {
		startWorker(p, workers)
	}
	return workers
}

func startWorker(p dependencies, workers *sync.Map) {
	worker := &Worker{
		Ctx:            p.WorkerCtx,
		Status:         WorkerStatusFree,
		ID:             fmt.Sprintf("%s", uuid.Must(uuid.NewV4())),
		TaskChan:       p.TaskChan,
		TaskResultChan: p.TaskResultChan,
		ErrorChan:      p.ErrorChan,
		WaitGroup:      p.WaitGroup,
		IdleTime:       p.WorkerIdleTime,
	}
	workers.Store(worker.ID, worker)
	p.WaitGroup.Add(1)
	go worker.Start()
}

type Manager struct {
	Ctx            context.Context
	WorkerCtx      context.Context
	ErrorChan      chan *Worker
	WaitGroup      *sync.WaitGroup
	Workers        *sync.Map
	TaskChan       chan *Task
	TaskResultChan chan *TaskResult
	WorkerNum      int
	MaxWorkerNum   int
	WorkerIdleTime time.Duration
}

func SyncMapLen(m *sync.Map) int {
	cnt := 0
	m.Range(func(k, v any) bool {
		cnt++
		return true
	})
	return cnt
}

func (m *Manager) Manage() {
	go func() {
		for {
			select {
			case <-m.Ctx.Done():
				log.Printf("[manager] expansion and contraction management exit\n")
				return
			default:
				if len(m.TaskChan) > 0 && SyncMapLen(m.Workers) < m.MaxWorkerNum {
					startWorker(dependencies{
						WorkerCtx:      m.WorkerCtx,
						TaskChan:       m.TaskChan,
						TaskResultChan: m.TaskResultChan,
						WaitGroup:      m.WaitGroup,
						ErrorChan:      m.ErrorChan,
						WorkerIdleTime: m.WorkerIdleTime,
					}, m.Workers)
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-m.Ctx.Done():
				log.Printf("[manager] error handling shutdown\n")
				return
			case worker := <-m.ErrorChan:
				if worker == nil {
					continue
				}
				if worker.Status == WorkerStatusUnavailable {
					log.Printf("[manager] worker %s error: %v\n", worker.ID, worker.Error)
					worker.Status = WorkerStatusFree
					worker.Error = nil
					go worker.Start()
				} else if worker.Status == WorkerStatusFree {
					if SyncMapLen(m.Workers) > m.WorkerNum {
						m.Workers.Delete(worker.ID)
						log.Printf("[manager] recall worker %s\n", worker.ID)
					}
				}
			}
		}
	}()
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-m.Ctx.Done():
				log.Printf("[manager] status checking shutdown\n")
				return
			case <-t.C:
				m.Workers.Range(func(k, v any) bool {
					val := v.(*Worker)
					log.Printf("[manager] worker %s status: %s\n", k, val.Status)
					return true
				})
			}
		}
	}()
}

func initManager(p dependencies) {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		Ctx:            ctx,
		WorkerCtx:      p.WorkerCtx,
		ErrorChan:      p.ErrorChan,
		WaitGroup:      p.WaitGroup,
		Workers:        p.WorkersMap,
		TaskChan:       p.TaskChan,
		TaskResultChan: p.TaskResultChan,
		MaxWorkerNum:   p.MaxWorkerNum,
		WorkerNum:      p.WorkerNum,
		WorkerIdleTime: p.WorkerIdleTime,
	}
	m.Manage()
	go func() {
		p.WaitGroup.Wait()
		cancel()
		close(p.ErrorChan)
	}()
}

func (p *Pool) ClosePool() error {
	workersReady := true
	p.Workers.Range(func(k, v any) bool {
		if v.(*Worker).Status == WorkerStatusBusy {
			workersReady = false
			return false
		}
		return true
	})
	if !workersReady {
		return errors.New("[pool] workers are busy, waiting for them to finish")
	}
	p.ShutDown()
	close(p.TaskResultChan)
	close(p.TaskChan)
	return nil
}

func InitPool(workerNum, maxWorkerNum int, workerIdleTime time.Duration) *Pool {
	if workerNum <= 0 {
		workerNum = runtime.NumCPU()
	}
	if maxWorkerNum <= 0 {
		maxWorkerNum = workerNum * 2
	}
	if maxWorkerNum < workerNum {
		maxWorkerNum = workerNum
	}
	if workerIdleTime <= 0 {
		workerIdleTime = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	taskChan := make(chan *Task, workerNum)
	taskResultChan := make(chan *TaskResult)
	errorChan := make(chan *Worker)
	wg := &sync.WaitGroup{}
	dep := dependencies{
		WorkerNum:      workerNum,
		MaxWorkerNum:   maxWorkerNum,
		WorkerCtx:      ctx,
		TaskChan:       taskChan,
		TaskResultChan: taskResultChan,
		ErrorChan:      errorChan,
		WaitGroup:      wg,
		WorkerIdleTime: workerIdleTime,
	}
	workers := initWorkers(dep)
	dep.WorkersMap = workers
	initManager(dep)
	return &Pool{
		TaskChan:       taskChan,
		TaskResultChan: taskResultChan,
		ShutDown:       cancel,
		Workers:        workers,
	}
}
