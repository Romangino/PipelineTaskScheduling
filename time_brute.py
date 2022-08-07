from itertools import permutations, product
from math import inf
import numpy as np
from random import sample
import timeit
from functools import partial
from matplotlib import pyplot as plt

from calc_makespan import calc_makespan, PipelineTask


def main():
    """ Solve increasingly large problems via brute force, time / graph it """
    task_a = PipelineTask(name="A", step=0, time_factor=4, space_factor=1, cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=6, space_factor=1, cpus=12)
    task_c = PipelineTask(name="C", step=2, time_factor=2, space_factor=2, cpus=4)
    tasks = [task_a, task_b, task_c]

    # Plot timings for brute force solutions to problem
    max_cpus = 4  # set constant number of cpus
    max_mem = 32  # set constant amount of memory
    n = 9  # time using from 1 to n input files
    repeats = 3  # three repeats (e.g. using different random input file sizes)

    all_times_order = [[0 for r in range(repeats)] for i in range(n)]
    all_times_cpus = [[0 for r in range(repeats)] for i in range(n)]
    all_times_both = [[0 for r in range(repeats)] for i in range(n)]

    for i in range(n):
        for r in range(repeats):
            print(i, r)
            file_sizes = sample(range(1, 20), i + 1)  # n random unique file sizes

            # Time brute force solution just optimizing input order (fixed cpus assigned to tasks)
            timer = timeit.Timer(partial(brute_force_order, file_sizes, max_mem, max_cpus, tasks))
            t = timer.repeat(repeat=3, number=1)
            all_times_order[i][r] = np.mean(t)  # mean of replicates for this random sample of i files

            # Time brute force solution just optimizing cpus assigned to tasks (fixed file order)
            timer = timeit.Timer(partial(brute_force_cpus, file_sizes, max_mem, max_cpus, tasks))
            t = timer.repeat(repeat=3, number=1)
            all_times_cpus[i][r] = np.mean(t)

            # Time brute force solution optimizing both input file order and cpus assigned to tasks
            timer = timeit.Timer(partial(brute_force, file_sizes, max_mem, max_cpus, tasks))
            t = timer.repeat(repeat=3, number=1)
            all_times_both[i][r] = np.mean(t)

    # Plot timings
    plt.figure(figsize=(9, 6), dpi=80)
    x = range(1, n + 1)

    # Mean run time over all random samples & their replicates
    y_order = [np.mean(times) for times in all_times_order]
    y_cpus = [np.mean(times) for times in all_times_cpus]
    y_both = [np.mean(times) for times in all_times_both]

    plt.plot(x, y_order, label="File Order")
    plt.plot(x, y_cpus, label="Task CPUs")
    plt.plot(x, y_both, label="Both")

    plt.xlabel("Input files")
    plt.xticks(range(1, n + 1))
    plt.ylabel("Time (seconds)")
    plt.legend(loc='upper left')
    # plt.show()
    plt.savefig("timing_brute_force.png")


def brute_force_order(file_sizes, max_memory, max_cpus, tasks):
    """
    Try all possible input orders of the given file_sizes to determine optimal order. Return the optimal makespan and
    the parameters that achieved it.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order listed; list of PipelineTask
    :return: the optimal makespan and the parameters used; (int, list of int)
    """
    file_order_perms = list(set(permutations(file_sizes)))  # get all unique permutations of the input files
    best_makespan = inf
    best_order = None

    for file_order in file_order_perms:
        makespan = calc_makespan(file_sizes=file_order, max_memory=max_memory, max_cpus=max_cpus, tasks=tasks)
        if makespan < best_makespan:
            best_makespan = makespan
            best_order = file_order

    return best_makespan, best_order


def brute_force_cpus(file_sizes, max_memory, max_cpus, tasks):
    """
    Try all possible task cpu assignments. Return the optimal makespan and the parameters that achieved it.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order listed; list of PipelineTask
    :return: the optimal makespan and the parameters used; (int, tuple of int)
    """

    cpu_assns = product(*[range(1, max_cpus + 1) for task in tasks])  # each task can use between 1 and the max cpus
    best_makespan = inf
    best_cpu_assn = None  # optimal assignment of cpus to tasks

    for cpu_assn in cpu_assns:
        # Set each task's reserved cpus
        for i, task in enumerate(tasks):
            task.cpus = cpu_assn[i]

        makespan = calc_makespan(file_sizes=file_sizes, max_memory=max_memory, max_cpus=max_cpus, tasks=tasks)
        if makespan < best_makespan:
            best_makespan = makespan
            best_cpu_assn = cpu_assn

    return best_makespan, best_cpu_assn


def brute_force(file_sizes, max_memory, max_cpus, tasks):
    """
    Try all possible input orders of the given file_sizes and all possible task cpu assignments.
    Return the optimal makespan and the parameters that achieved it.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order listed; list of PipelineTask
    :return: the optimal makespan and the parameters used; (int, tuple of tuples of ints)
    """
    file_order_perms = list(set(permutations(file_sizes)))  # get all unique permutations of the input files
    cpu_assns = product(*[range(1, max_cpus + 1) for task in tasks])  # each task can use between 1 and the max cpus
    all_params = product(file_order_perms, cpu_assns)  # file_sizes! * max_cpus^num_tasks possible solutions

    best_makespan = inf
    best_params = None

    for params in all_params:
        file_order = params[0]
        cpu_assn = params[1]

        # Set each task's reserved cpus
        for i, task in enumerate(tasks):
            task.cpus = cpu_assn[i]

        makespan = calc_makespan(file_sizes=file_order, max_memory=max_memory, max_cpus=max_cpus, tasks=tasks)
        if makespan < best_makespan:
            best_makespan = makespan
            best_params = params

    return best_makespan, best_params


if __name__ == "__main__":
    main()
