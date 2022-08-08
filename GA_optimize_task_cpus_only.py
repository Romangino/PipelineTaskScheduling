from numpy.random import randint
from numpy.random import rand
from math import inf

from calc_makespan import calc_makespan, PipelineTask
from time_brute import brute_force_cpus


def main():
    """ Run examples """
    parallel_func = lambda size, time, cpus: size * time // cpus ** (3/4)  # diminishing CPU returns
    task_a = PipelineTask(name="A", step=0, time_factor=7, space_factor=3, cpus=8, parallel_func=parallel_func)
    task_b = PipelineTask(name="B", step=1, time_factor=10, space_factor=1, cpus=12, parallel_func=parallel_func)
    task_c = PipelineTask(name="C", step=2, time_factor=3, space_factor=2, cpus=4, parallel_func=parallel_func)
    tasks = [task_a, task_b, task_c]

    best_makespan, best_cpu_assn = GA_cpus(file_sizes=[26, 42, 31, 19, 55, 11, 61], max_memory=200, max_cpus=64,
                                           tasks=tasks, rounds=1000, pop_size=100, crossover_rate=0.9,
                                           mutation_rate=0.2)
    # 3 tasks and 64 CPUs still doable by brute force
    opt_makespan, opt_cpu_assn = brute_force_cpus(file_sizes=[26, 42, 31, 19, 55, 11, 61], max_memory=200, max_cpus=64,
                                                  tasks=tasks)
    print(f"Actual optimal solution: {opt_makespan}, achieved using parameters {opt_cpu_assn}")


def tournament(pop, pop_size, makespans, num_competitors=3):
    """
    Perform a "tournament" between a given number of competitors randomly selected from the population
    and return the winner, i.e. the competitor with the lowest makespan.

    :param pop: the population; list of any
    :param pop_size: population size; int
    :param makespans: makespan for each individual in the population; list of int
    :param num_competitors: the number of competitors in a tournament; int
    :return: the individual which won the competition; an element of pop
    """
    winner_i = randint(pop_size)  # pick a random individual to compete
    winner = pop[winner_i]
    for i in randint(0, pop_size, num_competitors - 1):  # perform a competition against a random challenger
        if makespans[i] < makespans[winner_i]:
            winner_i, winner = i, pop[i]
    return winner


def crossover(parent1, parent2, crossover_rate):
    """
    Perform a crossover event between two parents with frequency proportional to the given crossover rate.

    :param parent1: an individual from the population; list of any
    :param parent2: an individual from the population; list of any
    :param crossover_rate: the rate at which crossover events should occur; float
    :return: two new offspring; list of same type as parents
    """
    if rand() < crossover_rate:  # perform a crossover event crossover_rate proportion of the time
        x = randint(1, len(parent1) - 1)  # crossover index
        c1 = parent1[:x] + parent2[x:]  # first part from parent 1, rest from parent 2
        c2 = parent2[:x] + parent1[x:]  # vice versa
    else:  # just copy parents
        c1, c2 = parent1.copy(), parent2.copy()

    return [c1, c2]


def mutate(individual, max_cpus, mutation_rate):
    """
    For each gene in an individual, create a random mutation (e.g. randomly change number of cpus assigned to that task
    to some other random valid number) with frequency proportional to the given mutation rate. Mutates the individual
    in place.

    :param individual: an individual from the population; list of int
    :param max_cpus: max cpus (mutation assigns random int between 1 and max cpus to ensure result is still a
    valid solution); int
    :param mutation_rate: the rate at which mutation events should occur; float
    """
    for i in range(len(individual)):  # for each gene
        if rand() < mutation_rate:  # perform a mutation event mutation_rate proportion of the time
            individual[i] = randint(1, max_cpus + 1)


def cpu_objective(cpu_assn, file_sizes, max_memory, max_cpus, tasks):
    """
    Objective function for optimizing task CPU assignment, i.e. run calc_makespan after setting cpus for each
    task according to cpu_assn.

    :param cpu_assn: a particular assignment of CPUs to tasks; tuple of int
    :return: the makespan for these parameters; int
    """
    # Set each task's reserved cpus
    for i, task in enumerate(tasks):
        task_cpus = cpu_assn[i]
        if task_cpus > max_cpus:
            raise Exception(f"Can't assign {task_cpus} to a task when the max is {max_cpus}")
        task.cpus = task_cpus

    return calc_makespan(file_sizes=file_sizes, max_memory=max_memory, max_cpus=max_cpus, tasks=tasks)


def GA_cpus(file_sizes, max_memory, max_cpus, tasks, rounds, pop_size, crossover_rate=0.9, mutation_rate=0.05):
    """
    A simple genetic algorithm to find an approximately optimal task CPU assignment for minimizing makespan of a
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
    num_tasks = len(tasks)

    # Create an initial population: an individual is a tuple of length num_tasks representing a CPU assignment
    # of from 1 to max_cpus to each task
    pop = [randint(1, max_cpus + 1, num_tasks).tolist() for x in range(pop_size)]

    # Store best (and worst) makespans found so far and CPU assignments that achieved them
    best_cpu_assn, best_makespan = None, inf
    worst_cpu_assn, worst_makespan = None, 0  # just for curiosity

    # Run the genetic algorithm for the specified number of rounds
    for round in range(rounds):
        # Score all individuals in the population
        makespans = [cpu_objective(cpu_assn, file_sizes, max_memory, max_cpus, tasks) for cpu_assn in pop]

        # Update best (and worst) solution found so far
        for i in range(pop_size):
            if makespans[i] <= best_makespan:
                best_cpu_assn, best_makespan = pop[i], makespans[i]
            if makespans[i] >= worst_makespan:
                worst_cpu_assn, worst_makespan = pop[i], makespans[i]

        print(f"Round {round}: best makespan {best_makespan} achieved with CPU assignment {best_cpu_assn}")

        # Create the next generation
        parents = [tournament(pop, pop_size, makespans) for x in range(pop_size)]
        next_gen = list()
        for i in range(0, pop_size, 2):
            # Mate two parents who each won a tournament to create two children
            parent1, parent2 = parents[i], parents[i + 1]
            children = crossover(parent1, parent2, crossover_rate)
            for child in children:
                mutate(child, max_cpus, mutation_rate)
                next_gen.append(child)

        pop = next_gen

    print(f"Final best makespan {best_makespan}, achieved using parameters {best_cpu_assn}")
    print(f"Final worst makespan {worst_makespan}, achieved using parameters {worst_cpu_assn}")

    return best_makespan, best_cpu_assn


if __name__ == "__main__":
    main()
