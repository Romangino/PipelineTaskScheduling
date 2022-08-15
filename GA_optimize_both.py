from numpy.random import randint
from numpy.random import rand
from random import sample
from math import inf

from calc_makespan import calc_makespan, PipelineTask
import GA_optimize_task_cpus_only as GA_cpus
import GA_optimize_file_order_only as GA_order
from time_brute import brute_force


def main():
    """ Run examples """
    task_a = PipelineTask(name="A", step=0, time_factor=7, space_factor=3, cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=10, space_factor=1, cpus=12)
    task_c = PipelineTask(name="C", step=2, time_factor=3, space_factor=2, cpus=4)
    tasks = [task_a, task_b, task_c]

    # Running on a relatively small input of files and CPUs compared to other examples since
    # brute force makes (4! * 64^3 = 6,291,456) calls to calc_makespan in this particular example
    best_makespan, best_params = GA_both(file_sizes=[22, 52, 45, 30],
                                         max_memory=200,
                                         max_cpus=64,
                                         tasks=tasks,
                                         rounds=1000,
                                         pop_size=100,
                                         crossover_rate=0.9,
                                         mutation_rate=0.2)

    opt_makespan, opt_params = brute_force(file_sizes=[22, 52, 45, 30],
                                           max_memory=200,
                                           max_cpus=64,
                                           tasks=tasks)

    print(f"Actual optimal solution: {opt_makespan}, achieved using parameters {opt_params}")


def crossover(parent1, parent2, crossover_rate):
    """
    Perform a crossover event between two parents with frequency proportional to the given crossover rate.

    :param parent1: an individual from the population; list of any
    :param parent2: an individual from the population; list of any
    :param crossover_rate: the rate at which crossover events should occur;
    float
    :return: two new offspring; list of same type as parents
    """

    # perform a crossover event crossover_rate proportion of the time
    if rand() < crossover_rate:

        # mutation rate here is 1 to ensure a crossover in the children
        order_children = GA_order.crossover(parent1[0], parent2[0], 1)      # crossover file orders
        cpu_children = GA_cpus.crossover(parent1[1], parent2[1], 1)         # crossover cpus

        # create children based off of both crossovers
        c1 = [order_children[0], cpu_children[0]]
        c2 = [order_children[1], cpu_children[1]]

    else:  # just copy parents
        c1, c2 = parent1.copy(), parent2.copy()

    return [c1, c2]


def mutate(individual, max_cpus, mutation_rate):
    """
    For each gene in an individual, create a random mutation. For file orders, randomly swap the assignment of
    one file with another. For Task CPU assignment, randomly change number of CPUs assigned to that task
    to some other random valid number. Mutation occurs with frequency proportional to the given mutation rate.
    Mutates the individual in place.

    :param individual: an individual from the population; list of int
    :param max_cpus: max cpus (Task CPU mutation assigns random int between 1 and max cpus to ensure result is
    still a valid solution); int
    :param mutation_rate: the rate at which mutation events should occur; float
    """

    # Call mutation functions with given mutation rate
    GA_order.mutate(individual[0], mutation_rate)
    GA_cpus.mutate(individual[1], max_cpus, mutation_rate)


def objective(individual, max_memory, max_cpus, tasks):
    """
    Objective function for optimizing file order and task CPU assignment i.e. run calc_makespan after setting
    cpus for each task according to the individual. File orders are already reordered and are used as file
    sizes.

    :param individual: a particular assignment of file ordering and CPUs to tasks; list of lists
    :return: the makespan for these parameters; int
    """
    # Set cpus per task according to
    for i in range(len(tasks)):
        task_cpus = individual[1][i]

        if task_cpus > max_cpus:
            raise Exception(f"Can't assign {task_cpus} to a task when the max is {max_cpus}")
        tasks[i].cpus = task_cpus

    return calc_makespan(file_sizes=individual[0],
                         max_memory=max_memory,
                         max_cpus=max_cpus,
                         tasks=tasks)


def GA_both(file_sizes, max_memory, max_cpus, tasks, rounds, pop_size, crossover_rate, mutation_rate):
    """
    A simple genetic algorithm to find an approximately optimal file ordering and Task CPU assignment for
    minimizing makespan of a pipeline.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order listed; list of PipelineTask
    :param rounds: number of rounds to run the genetic algorithm; int
    :param pop_size: population size; int
    :param crossover_rate: proportion of the time a crossover event occurs; float
    :param mutation_rate: proportion of the time a mutation event occurs; float
    :return: the lowest makespan and the file ordering / assignment of cpus to tasks that achieved it; list of
    lists
    """
    num_tasks = len(tasks)
    num_files = len(file_sizes)

    # Create an initial population: an individual is a list of 2 lists where
    # first represents a list of file orderings for the given number of files, length num_files
    pop_order = [sample(file_sizes, num_files) for x in range(pop_size)]

    # second represents a CPU assignment of from 1 to max_cpus to each task, length num_tasks
    pop_cpu = [randint(1, max_cpus + 1, num_tasks).tolist() for x in range(pop_size)]

    # create a 1 to 1 matching of both lists to create all individuals in the population
    pop = []
    for i in range(pop_size):
        individual = [pop_order[i], pop_cpu[i]]

        pop.append(individual)

    # Store best (and worst) makespans found so far and CPU assignments that achieved them
    best_params, best_makespan = None, inf
    worst_params, worst_makespan = None, 0

    # Run the genetic algorithm for the specified number of rounds
    for round in range(rounds):
        # Score all the individuals in the population
        makespans = [objective(individual, max_memory, max_cpus, tasks) for individual in pop]

        # Update best (and worst) solution found so far
        for i in range(pop_size):
            if makespans[i] <= best_makespan:
                best_params, best_makespan = pop[i], makespans[i]

            if makespans[i] >= worst_makespan:
                worst_params, worst_makespan = pop[i], makespans[i]

        print(f"Round {round}: best makespan {best_makespan} achieved with params {best_params}")

        # Create the next generation
        parents = [GA_cpus.tournament(pop, pop_size, makespans) for x in range(pop_size)]

        next_gen = list()
        for i in range(0, pop_size, 2):
            parent1, parent2 = parents[i], parents[i + 1]
            children = crossover(parent1, parent2, crossover_rate)
            for child in children:
                mutate(child, max_cpus, mutation_rate)
                next_gen.append(child)

        pop = next_gen

    print(f"Final best makespan {best_makespan}, achieved using parameters {best_params}")
    print(f"Final worst makespan {worst_makespan}, achieved using parameters {worst_params}")

    return best_makespan, best_params


if __name__ == "__main__":
    main()
