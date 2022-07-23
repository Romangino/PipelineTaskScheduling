"""
Final Project
Pipeline Scheduling Optimization

Liz Codd, Rachel Dao, Evan Haines, Gino Romanello
7/23/22
"""


def main():
    """ Run examples """
    bbduk = PipelineTask("BBDuk", 10, 1, 8)
    fastqc = PipelineTask("FastQC", 5, 1, 4)
    bwa = PipelineTask("BWA", 50, 2, 16)
    umi_tools = PipelineTask("UMI-tools", 60, 10, 1)
    calc_makespan(file_sizes=[3, 4, 8, 10, 5, 2, 1, 1, 3, 10],
                  max_memory=200,
                  max_cpus=60,
                  tasks=[bbduk, fastqc, bwa, umi_tools])


def calc_makespan(file_sizes, max_memory, max_cpus, tasks):
    """
    Calculates the duration of each task for each file, then uses this
    information to calculate the total makespan of the pipeline assuming the
    files are processed in the given order, tasks must be completed in the
    given order, and each task starts as soon as the resources become available.

    :param file_sizes: size of the files rounded to the nearest unit; int
    :param max_memory: the memory limits of the machine; int
    :param max_cpus: the number of cores in the machine; int
    :param tasks: a list of tasks to be completed for each file in the order
    listed; list of PipelineTask
    :return: the makespan of the pipeline in the same units given in the tasks;
    int
    """




class PipelineTask:
    """
    A class to contain information and methods for a pipeline task
    """
    def __init__(self, name, time_factor, space_factor, cpus):
        """
        Construct a new instance of PipelineTask.

        :param name: name of the task; str
        :param time_factor: time units required to complete this task for a
        file size of 1 unit using 1 CPU. e.g. input of 50 means "50 minutes for
        a 1 Gb file using 1 CPU"; float
        :param space_factor: the peak RAM required to process a file size of 1
        unit; int
        :param cpus: the number of CPUs designated for this task; int
        """
        self.name = name
        self.time_factor = time_factor
        self.space_factor = space_factor
        self.cpus = cpus

    def calc_duration(self, file_size):
        """
        Calculate the time of completion of this task given an input file size
        :param file_size: a file size rounded to the nearest unit; int
        :return: time of completion in the same time units as time_factor; int
        """
        return round(file_size * self.time_factor / self.cpus)


if __name__ == "__main__":
    main()
