package backfill

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type benchScale struct {
	name         string
	queues       int
	jobsPerQueue int
	tasksPerJob  int
}

type backfillBenchFixture struct {
	test   *uthelper.TestCommonStruct
	action *Action
	actx   *backfillContext
}

type allocateResourcesStrategy struct {
	popAllJobs  bool
	popAllTasks bool
}

func BenchmarkBackfillAllocateResources(b *testing.B) {
	scales := []benchScale{
		{name: "q2_j4_t16", queues: 2, jobsPerQueue: 4, tasksPerJob: 16},
		{name: "q4_j8_t16", queues: 4, jobsPerQueue: 8, tasksPerJob: 16},
	}

	for _, scale := range scales {
		scale := scale

		b.Run(scale.name+"/job_strategy/pop_all_jobs", func(b *testing.B) {
			benchmarkAllocateResourcesStrategy(b, scale, allocateResourcesStrategy{
				popAllJobs:  true,
				popAllTasks: false,
			})
		})
		b.Run(scale.name+"/job_strategy/pop_one_job_requeue_queue", func(b *testing.B) {
			benchmarkAllocateResourcesStrategy(b, scale, allocateResourcesStrategy{
				popAllJobs:  false,
				popAllTasks: false,
			})
		})

		b.Run(scale.name+"/task_strategy/pop_all_tasks", func(b *testing.B) {
			benchmarkAllocateResourcesStrategy(b, scale, allocateResourcesStrategy{
				popAllJobs:  true,
				popAllTasks: true,
			})
		})
		b.Run(scale.name+"/task_strategy/job_ready_break_requeue", func(b *testing.B) {
			benchmarkAllocateResourcesStrategy(b, scale, allocateResourcesStrategy{
				popAllJobs:  true,
				popAllTasks: false,
			})
		})
	}
}

func benchmarkAllocateResourcesStrategy(b *testing.B, scale benchScale, strategy allocateResourcesStrategy) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fixture := newBackfillBenchFixture(scale)
		runtime.GC()
		oldGCPercent := debug.SetGCPercent(-1)
		b.StartTimer()

		allocateResourcesWithStrategy(fixture.action, fixture.actx, strategy)

		b.StopTimer()
		debug.SetGCPercent(oldGCPercent)
		fixture.close()
	}
}

func newBackfillBenchFixture(scale benchScale) *backfillBenchFixture {
	pods, podGroups, queues := buildBenchWorkload(scale)
	totalTasks := scale.queues * scale.jobsPerQueue * scale.tasksPerJob
	node := util.BuildNode(
		"bench-node-0",
		api.BuildResourceList(
			"100000",
			"500Gi",
			[]api.ScalarResource{{Name: "pods", Value: fmt.Sprintf("%d", totalTasks+1000)}}...,
		),
		map[string]string{},
	)

	test := &uthelper.TestCommonStruct{
		Name:      "backfill benchmark",
		Pods:      pods,
		Nodes:     []*v1.Node{node},
		PodGroups: podGroups,
		Queues:    queues,
	}

	ssn := test.RegisterSession(nil, nil)
	action := New()
	action.session = ssn
	actx := action.buildBackfillContext()

	return &backfillBenchFixture{
		test:   test,
		action: action,
		actx:   actx,
	}
}

func buildBenchWorkload(scale benchScale) ([]*v1.Pod, []*schedulingv1beta1.PodGroup, []*schedulingv1beta1.Queue) {
	totalJobs := scale.queues * scale.jobsPerQueue
	totalTasks := totalJobs * scale.tasksPerJob
	pods := make([]*v1.Pod, 0, totalTasks)
	podGroups := make([]*schedulingv1beta1.PodGroup, 0, totalJobs)
	queues := make([]*schedulingv1beta1.Queue, 0, scale.queues)

	for q := 0; q < scale.queues; q++ {
		queueName := fmt.Sprintf("q-%d", q)
		queues = append(queues, util.BuildQueue(queueName, 1, nil))

		for j := 0; j < scale.jobsPerQueue; j++ {
			pgName := fmt.Sprintf("pg-%d-%d", q, j)
			podGroups = append(podGroups, util.BuildPodGroup(pgName, "default", queueName, 0, nil, schedulingv1beta1.PodGroupInqueue))

			for t := 0; t < scale.tasksPerJob; t++ {
				podName := fmt.Sprintf("%s-t-%d", pgName, t)
				pods = append(pods, util.BuildPod(
					"default",
					podName,
					"",
					v1.PodPending,
					api.BuildResourceList("0", "0"),
					pgName,
					map[string]string{},
					map[string]string{},
				))
			}
		}
	}

	return pods, podGroups, queues
}

func (f *backfillBenchFixture) close() {
	f.test.Close()
}

func allocateResourcesWithStrategy(backfill *Action, actx *backfillContext, strategy allocateResourcesStrategy) {
	ssn := backfill.session

	queues := actx.queues
	for !queues.Empty() {
		queue := queues.Pop().(*api.QueueInfo)

		jobQueue, found := actx.jobsByQueue[queue.UID]
		if !found || jobQueue.Empty() {
			continue
		}

		if strategy.popAllJobs {
			for !jobQueue.Empty() {
				job := jobQueue.Pop().(*api.JobInfo)
				scheduleJobWithTaskStrategy(backfill, ssn, jobQueue, actx.taskQueueByJob, job, strategy.popAllTasks)
			}
			continue
		}

		job := jobQueue.Pop().(*api.JobInfo)
		taskQueue, found := actx.taskQueueByJob[job.UID]
		if !found || taskQueue.Empty() {
			queues.Push(queue)
			continue
		}

		scheduleJobWithTaskStrategy(backfill, ssn, jobQueue, actx.taskQueueByJob, job, strategy.popAllTasks)
		queues.Push(queue)
	}
}

func scheduleJobWithTaskStrategy(
	backfill *Action,
	ssn *framework.Session,
	jobQueue *util.PriorityQueue,
	taskQueueByJob map[api.JobID]*util.PriorityQueue,
	job *api.JobInfo,
	popAllTasks bool,
) {
	taskQueue, found := taskQueueByJob[job.UID]
	if !found || taskQueue.Empty() {
		return
	}

	stmt := framework.NewStatement(ssn)
	if popAllTasks {
		allocateTasksForJobWithoutReadyBreak(backfill, stmt, job, taskQueue)
	} else {
		backfill.allocateTasksForJob(stmt, job, taskQueue)
	}

	if len(stmt.Operations()) > 0 && ssn.JobReady(job) {
		stmt.Commit()
		if !taskQueue.Empty() {
			jobQueue.Push(job)
		}
	} else {
		stmt.Discard()
	}
}

func allocateTasksForJobWithoutReadyBreak(backfill *Action, stmt *framework.Statement, job *api.JobInfo, taskQueue *util.PriorityQueue) {
	ssn := backfill.session
	predicateFunc := ssn.PredicateForAllocateAction
	ph := util.NewPredicateHelper()

	for !taskQueue.Empty() {
		task := taskQueue.Pop().(*api.TaskInfo)

		fe := api.NewFitErrors()
		if err := ssn.PrePredicateFn(task); err != nil {
			for _, ni := range ssn.Nodes {
				fe.SetNodeError(ni.Name, err)
			}
			job.NodesFitErrors[task.UID] = fe
			continue
		}

		predicateNodes, fitErrors := ph.PredicateNodes(task, ssn.NodeList, predicateFunc, backfill.enablePredicateErrorCache, ssn.NodesInShard)
		if len(predicateNodes) == 0 {
			job.NodesFitErrors[task.UID] = fitErrors
			continue
		}

		node := predicateNodes[0]
		if len(predicateNodes) > 1 {
			candidateNodes := util.GetPredicatedNodeByShard(predicateNodes, ssn.NodesInShard)
			for _, nodes := range candidateNodes {
				nodeScores := util.PrioritizeNodes(task, nodes, ssn.BatchNodeOrderFn, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)
				node = ssn.BestNodeFn(task, nodeScores)
				if node == nil {
					node, _ = util.SelectBestNodeAndScore(nodeScores)
				}
				if node != nil {
					break
				}
			}
		}

		if err := stmt.Allocate(task, node); err != nil {
			if rollbackErr := stmt.UnAllocate(task); rollbackErr != nil {
				job.NodesFitErrors[task.UID] = fe
			}
			fe.SetNodeError(node.Name, err)
			job.NodesFitErrors[task.UID] = fe
			continue
		}

		metrics.UpdateE2eSchedulingDurationByJob(job.Name, string(job.Queue), job.Namespace, metrics.Duration(job.CreationTimestamp.Time))
		metrics.UpdateE2eSchedulingLastTimeByJob(job.Name, string(job.Queue), job.Namespace, time.Now())
	}
}
