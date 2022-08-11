from calc_makespan import calc_makespan, PipelineTask


def main():
    """
    Purpose is to test functionality of calc_makespan and validate results
    as well as test edge-cases.
    """

    """
    Tests for Gantt Chart
    """
    # Test 1
    task_a = PipelineTask(name="A", step=0, time_factor=4,
                          space_factor=1,
                          cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=6,
                          space_factor=1,
                          cpus=12)

    assert 30 == calc_makespan(file_sizes=[20, 10, 10],
                               max_memory=32,
                               max_cpus=20,
                               tasks=[task_a, task_b])

    # Test 2
    task_a = PipelineTask(name="A", step=0, time_factor=2,
                          space_factor=1,
                          cpus=4)
    task_b = PipelineTask(name="B", step=1, time_factor=5,
                          space_factor=1,
                          cpus=6)

    assert 33 == calc_makespan(file_sizes=[25, 15, 10, 5],
                               max_memory=64,
                               max_cpus=16,
                               tasks=[task_a, task_b])

    # Test 3
    task_a = PipelineTask(name="A", step=0, time_factor=6,
                          space_factor=1,
                          cpus=4)
    task_b = PipelineTask(name="B", step=1, time_factor=4,
                          space_factor=2,
                          cpus=6)
    task_c = PipelineTask(name="C", step=2, time_factor=8,
                          space_factor=1,
                          cpus=8)

    assert 27 == calc_makespan(file_sizes=[2, 4, 6, 8],
                               max_memory=32,
                               max_cpus=16,
                               tasks=[task_a, task_b, task_c])

    """
    Test incorrect task sequence input
    Seems to change makespan the order in which tasks are arranged in list.

    -   Fixed by sorting tasks by step beforehand.
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
     Skipping a step and multiple steps with the same value

    Stuck in loop, program does not finish

    - Fixed. Returns -1 when step order is missing a step or step value 
    repeats.
    """
    task_a = PipelineTask(name="Task A", step=0, time_factor=2,
                          space_factor=1, cpus=2)
    task_c = PipelineTask(name="Task C", step=2, time_factor=2,
                          space_factor=1, cpus=2)
    assert -1 == calc_makespan([10, 10, 20],
                               max_memory=32,
                               max_cpus=32,
                               tasks=[task_a, task_c])

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


if __name__ == '__main__':
    main()
