"""
Final Project
Pipeline Scheduling Optimization

Liz Codd, Rachel Dao, Evan Haines, Gino Romanello
7/23/22
"""
from math import inf, log2, sqrt


def main():
    """ Run examples """
    # Tests
    task_a = PipelineTask(name="A", step=0, time_factor=4, space_factor=1, cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=6, space_factor=1, cpus=12)

    assert -1 == calc_makespan(file_sizes=[20, 10, 10],
                               max_memory=40,
                               max_cpus=10,  # not enough CPUs for task B
                               tasks=[task_a, task_b])

    assert 30 == calc_makespan(file_sizes=[20, 10, 10],
                               max_memory=32,
                               max_cpus=20,
                               tasks=[task_a, task_b])

    assert 20 == calc_makespan(file_sizes=[20, 10, 10],
                               max_memory=40,
                               max_cpus=32,
                               tasks=[task_a, task_b])

    """
    Tests for Gantt Chart
    """

    # Test 1
    task_a = PipelineTask(name="A", step=0, time_factor=4, space_factor=1,
                          cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=6, space_factor=1,
                          cpus=12)

    assert 30 == calc_makespan(file_sizes=[20, 10, 10],
                               max_memory=32,
                               max_cpus=20,
                               tasks=[task_a, task_b])

    # Test 2
    task_a = PipelineTask(name="A", step=0, time_factor=2, space_factor=1,
                          cpus=4)
    task_b = PipelineTask(name="B", step=1, time_factor=5, space_factor=1,
                          cpus=6)

    assert 33 == calc_makespan(file_sizes=[25, 15, 10, 5],
                               max_memory=64,
                               max_cpus=16,
                               tasks=[task_a, task_b])

    # Test 3
    task_a = PipelineTask(name="A", step=0, time_factor=6, space_factor=1,
                          cpus=4)
    task_b = PipelineTask(name="B", step=1, time_factor=4, space_factor=2,
                          cpus=6)
    task_c = PipelineTask(name="C", step=2, time_factor=8, space_factor=1,
                          cpus=8)

    assert 27 == calc_makespan(file_sizes=[2, 4, 6, 8],
                               max_memory=32,
                               max_cpus=16,
                               tasks=[task_a, task_b, task_c])

    """
    CPU overload
    Return -1 since not enough CPUs for program
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=1, cpus=33)
    assert -1 == calc_makespan([10, 10, 20],
                               max_memory=32,
                               max_cpus=32,
                               tasks=[task_a])

    """
    Memory overload
    Return -1 since not enough memory for program
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=2, cpus=33)
    assert -1 == calc_makespan([10, 20],
                               max_memory=9,
                               max_cpus=32,
                               tasks=[task_a])

    """
    Max CPU for both task.
    Returns 0. Is this the correct makespan need to make gantt chart
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=1,
                          space_factor=1, cpus=32)
    task_b = PipelineTask(name="Task B", step=1, time_factor=1,
                          space_factor=1, cpus=32)
    a = calc_makespan([10, 10, 20],
                      max_memory=32,
                      max_cpus=32,
                      tasks=[task_a, task_b])
    print("Max CPU:", a)

    """
    Test incorrect task sequence input
    Seems to change makespan the order in which tasks are arranged in list.
    
    -   Fixed by sorting tasks by step before hand.
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=1, cpus=2)
    task_b = PipelineTask(name="Task B", step=1, time_factor=2,
                          space_factor=1, cpus=2)
    taskAtoB = calc_makespan([10, 10, 20],
                             max_memory=32,
                             max_cpus=32,
                             tasks=[task_a, task_b])
    taskBtoA = calc_makespan([10, 10, 20],
                             max_memory=32,
                             max_cpus=32,
                             tasks=[task_b, task_a])
    assert taskAtoB == taskBtoA

    """
     Skipping a step
    
    Stuck in loop, program does not finish
    
    - Fixed. Returns -1 when step order is missing a step.
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=1, cpus=2)
    task_c = PipelineTask(name="Task C", step=2, time_factor=2,
                          space_factor=1, cpus=2)
    assert -1 == calc_makespan([10, 10, 20],
                               max_memory=32,
                               max_cpus=32,
                               tasks=[task_a, task_c])

    """
    Multiple steps with the same value
    
    Stuck in loop
    
    - Fixed. Return -1 when steps have matching values
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=1, cpus=2)
    task_b = PipelineTask(name="Task B", step=0, time_factor=2,
                          space_factor=1, cpus=2)
    task_c = PipelineTask(name="Task C", step=2, time_factor=2,
                          space_factor=1, cpus=2)

    assert -1 == calc_makespan([10, 10, 20],
                               max_memory=32,
                               max_cpus=32,
                               tasks=[task_a, task_b, task_c])

    # More realistic example (but a little too big to work out by hand)
    bbduk = PipelineTask(name="BBDuk", step=0, time_factor=10, space_factor=1, cpus=8)
    fastqc = PipelineTask(name="FastQC", step=1, time_factor=5, space_factor=1, cpus=4)
    bwa = PipelineTask(name="BWA", step=2, time_factor=50, space_factor=2, cpus=16)
    umi_tools = PipelineTask(name="UMI-tools", step=3, time_factor=60, space_factor=10, cpus=1)
    print(calc_makespan(file_sizes=[3, 4, 8, 10, 5, 2, 1, 1, 3, 10],
                        max_memory=200,
                        max_cpus=60,
                        tasks=[bbduk, fastqc, bwa, umi_tools]))


def calc_makespan(file_sizes, max_memory, max_cpus, tasks):
    """
    Calculates the duration of each task for each file, then uses this
    information to calculate the total makespan of the pipeline assuming the
    files are processed in the given order, tasks must be completed in the
    given order, and the next task starts as soon as enough resources become available.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order
    listed; list of PipelineTask
    :return: the makespan of the pipeline in the same units given in the tasks;
    int
    """
    num_steps = len(tasks)
    num_samples = len(file_sizes)
    samples = [Sample(i, file_sizes[i]) for i in range(num_samples)]

    # Sorting task by step number beforehand
    tasks.sort(key=lambda task: task.step)

    # Return -1 if any step is invalid.
    for i in range(len(tasks)):
        if tasks[i].step != i:
            return -1

    # All jobs to be scheduled, e.g. row i column j contains ith task for the jth sample
    all_jobs = [[Job(sample, task) for sample in samples] for task in tasks]
    num_remaining_jobs = num_steps * num_samples

    # Return -1 if any job is impossible
    for step in all_jobs:
        for job in step:
            if job.memory > max_memory or job.cpus > max_cpus:
                return -1

    # Resources currently available
    available_mem = max_memory
    available_cpus = max_cpus

    schedule = []  # list of scheduled jobs
    makespan = 0  # latest end time of a job in schedule
    t = 0  # initialize a clock

    # Keep the clock running until all jobs finish
    while num_remaining_jobs > 0:
        next_t = inf  # update this to track when next running job ends (so we can jump to that time point)

        # Check which running jobs have finished, give back their resources, mark the step as completed, and
        # update next time point
        for job in schedule:
            if job.end_time == t:
                job.is_running = False
                available_cpus += job.cpus
                available_mem += job.memory
                job.sample.steps_completed += 1
                num_remaining_jobs -= 1
            elif job.end_time > t and job.is_running:
                next_t = min(next_t, job.end_time)

        # Go through jobs and schedule next runnable job, decreasing resources and updating next_t and makespan
        for step in all_jobs:
            for job in step:
                if not job.is_running and job.sample.steps_completed == job.task.step and \
                        job.cpus <= available_cpus and job.memory <= available_mem:
                    job.is_running = True
                    job.start_time = t
                    job.end_time = t + job.duration

                    available_cpus -= job.cpus
                    available_mem -= job.memory

                    next_t = min(next_t, job.end_time)
                    makespan = max(makespan, job.end_time)
                    schedule.append(job)

        t = next_t  # move clock forward

    return makespan


class PipelineTask:
    """
    A class to contain information and methods for a pipeline task
    """

    def __init__(self, name, step, time_factor, space_factor, cpus,
                 parallel_func=lambda size, time, cpus: size * time // cpus):
        """
        Construct a new instance of class PipelineTask.

        :param name: name of the task; str
        :param step: which step the task is in the pipeline; int
        :param time_factor: time units required to complete this task for a
        file size of 1 unit using 1 CPU. e.g. input of 50 means "50 minutes for
        a 1 Gb file using 1 CPU"; int
        :param space_factor: the peak RAM required to process a file size of 1
        unit; int
        :param cpus: the number of CPUs designated for this task; int
        """
        self.name = name
        self.step = step
        self.time_factor = time_factor
        self.space_factor = space_factor
        self.cpus = cpus
        self.parallel_func = parallel_func

    def __repr__(self):
        return f"Task #{self.step}: {self.name}"


class Job:
    """
    A class to contain information about a specific job
    """

    def __init__(self, sample, task):
        """
        Construct a new instance of class Job.

        :param sample: the sample being processed; Sample
        :param task: the task being performed; PipelineTask
        """
        self.sample = sample
        self.task = task
        # self.duration = sample.file_size * task.time_factor // task.cpus ** (3/4)
        self.duration = task.parallel_func(sample.file_size, task.time_factor, task.cpus)
        self.memory = sample.file_size * task.space_factor
        self.cpus = task.cpus
        self.is_running = False
        self.start_time = None
        self.end_time = None

    def __repr__(self):
        return f"Sample {self.sample.sample_id}, PipelineTask {self.task.name}"


class Sample:
    """
    A class to contain information about a sample to be run through the pipeline.
    """

    def __init__(self, sample_id, file_size):
        """
        Construct a new instance of class Sample.

        :param sample_id: id of the sample; int
        :param file_size: size of the input file; int
        """
        self.sample_id = sample_id
        self.file_size = file_size
        self.steps_completed = 0

    def __repr__(self):
        return f"Sample {self.sample_id}, size {self.file_size}"


if __name__ == "__main__":
    main()
