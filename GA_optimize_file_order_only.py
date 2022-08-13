from numpy.random import randint
from numpy.random import rand
from random import sample
from math import inf

from calc_makespan import calc_makespan, PipelineTask
from GA_optimize_task_cpus_only import tournament
from time_brute import brute_force_order


def main():
    """ Run examples """
    task_a = PipelineTask(name="A", step=0, time_factor=7, space_factor=3, cpus=8)
    task_b = PipelineTask(name="B", step=1, time_factor=10, space_factor=1, cpus=12)
    task_c = PipelineTask(name="C", step=2, time_factor=3, space_factor=2, cpus=4)
    tasks = [task_a, task_b, task_c]

    best_makespan, best_file_order = GA_file_order(file_sizes=[26, 42, 31, 19, 55, 11, 61],
                                                   max_memory=200,
                                                   max_cpus=64,
                                                   tasks=tasks,
                                                   rounds=1000,
                                                   pop_size=100,
                                                   crossover_rate=0.9,
                                                   mutation_rate=0.2)

    # 7 files (7! possible orderings) doable in reasonable time by brute force
    opt_makespan, opt_file_order = brute_force_order(file_sizes=[26, 42, 31, 19, 55, 11, 61],
                                                     max_memory=200,
                                                     max_cpus=64,
                                                     tasks=tasks)

    print(f"Actual optimal solution: {opt_makespan}, achieved using parameters {opt_file_order}")


def crossover(parent1, parent2, crossover_rate):
    """
    Perform a crossover event between two parents with frequency proportional to the given crossover rate.

    :param parent1: an individual from the population; list of any
    :param parent2: an individual from the population; list of any
    :param crossover_rate: the rate at which crossover events should occur; float
    :return: two new offspring; list of same type as parents
    """

    # perform a crossover event crossover_rate proportion of the time
    if rand() < crossover_rate:
        x = randint(1, len(parent1) - 1)  # crossover index

        c1 = parent1[:x]                # first part from parent 1

        for i in range(len(parent2)):   # take remaining file indices not present in parent 1 in the
            if parent2[i] not in c1:    # order that they are present in parent 2
                c1.append(parent2[i])

        c2 = parent2[:x]                # vice versa

        for i in range(len(parent1)):
            if parent1[i] not in c2:
                c2.append(parent1[i])

    else:  # just copy parents
        c1, c2 = parent1.copy(), parent2.copy()

    return [c1, c2]


def mutate(individual, mutation_rate):
    """
    For each gene in an individual, create a random mutation (e.g. randomly swap the assignment of one file
    with another) with frequency proportional to the given mutation rate. Mutates the individual in place.

    :param individual: an individual from the population; list of int
    :param mutation_rate: the rate at which mutation events should occur; float
    """

    for i in range(len(individual)):  # for each gene

        # perform a mutation event mutation_rate proportion of the time
        if rand() < mutation_rate:
            swapIndex = i
            while swapIndex == i:
                swapIndex = randint(len(individual))

            individual[i], individual[swapIndex] = individual[swapIndex], individual[i]


def file_order_objective(file_order_assn, max_memory, max_cpus, tasks):
    """
    Objective function for optimizing file order assignment, i.e. simply run calc_makespan on file_order_assn
    since it contains files that are already reordered

    :param file_order_assn: a particular ordering of files; list of int
    :return: the makespan for these parameters; int
    """

    return calc_makespan(file_sizes=file_order_assn,
                         max_memory=max_memory,
                         max_cpus=max_cpus,
                         tasks=tasks)


def GA_file_order(file_sizes, max_memory, max_cpus, tasks, rounds, pop_size, crossover_rate=0.9,
                  mutation_rate=0.05):
    """
    A simple genetic algorithm to find an approximately optimal file ordering for minimizing makespan of a
    pipeline.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order listed; list of PipelineTask
    :param rounds: number of rounds to run the genetic algorithm; int
    :param pop_size: population size; int
    :param crossover_rate: proportion of the time a crossover event occurs; float
    :param mutation_rate: proportion of the time a mutation event occurs; float
    :return: the lowest makespan and the assignment of cpus to tasks that achieved it; int, tuple of ints
    """
    num_files = len(file_sizes)

    # Create an initial population:
    # an individual is a randomly generated list of file orderings for the given number of files
    pop = [sample(file_sizes, num_files) for x in range(pop_size)]

    # Store best (and worst) makespans found so far and file orders that achieved them
    best_file_order, best_makespan = None, inf
    worst_file_order, worst_makespan = None, 0

    # Run the genetic algorithm for the specified number of rounds
    for round in range(rounds):
        # Score all individuals in the population
        makespans = [file_order_objective(file_order_assn,
                                          max_memory,
                                          max_cpus,
                                          tasks) for file_order_assn in pop]

        # Update best (and worst) solution found so far
        # Remapping indexes of file_order_assn to actual file sizes
        for i in range(pop_size):
            if makespans[i] <= best_makespan:
                best_file_order, best_makespan = pop[i], makespans[i]

            if makespans[i] >= worst_makespan:
                worst_file_order, worst_makespan = pop[i], makespans[i]

        print(f"Round {round}: best makespan {best_makespan} achieved with file order assignment {best_file_order}")

        # Create the next generation
        parents = [tournament(pop, pop_size, makespans) for x in range(pop_size)]

        next_gen = list()
        for i in range(0, pop_size, 2):
            parent1, parent2 = parents[i], parents[i + 1]
            children = crossover(parent1, parent2, crossover_rate)
            for child in children:
                mutate(child, mutation_rate)
                next_gen.append(child)

        pop = next_gen

    print(f"Final best makespan {best_makespan}, achieved using parameters {best_file_order}")
    print(f"Final worst makespan {worst_makespan}, achieved using parameters {worst_file_order}")

    return best_makespan, best_file_order


if __name__ == "__main__":
    main()
