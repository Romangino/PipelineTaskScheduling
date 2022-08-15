import math
import calc_makespan as ms
import random as ran
from matplotlib import pyplot as plt

def swap(sched, i, j):
  '''
  Swaps elements at indices i and j in list sched.

  params:
  i - index of first element
  j - index of second element
  sched - the list whose elements are being swapped

  returns - a shallow copy of sched with appropriate 
    elements swapped
  '''
  out = sched.copy()
 
  job_1 = sched[i]
  job_2 = sched[j]

  out[i] = job_2
  out[j] = job_1

  return out


def create_plot(jobs, max_memory, max_cpus, tasks, T=500, r=0.99, L=5, T_min = 0.2):

  temp_array = []
  makespan_array = []
  simulated_annealing(jobs, max_memory, max_cpus, tasks, temp_array, makespan_array, T, r, L, T_min)
  plt.plot(temp_array, makespan_array)
  ax = plt.gca()
  ax.invert_xaxis()
  plt.ylabel("Makespan")
  plt.xlabel("Temperature")
  plt.title("Temperature vs Makespan")
  plt.show()



def simulated_annealing(jobs, max_memory, max_cpus, tasks, temp_arr=[], makespan_arr=[], T=500, r=0.99, L=5, T_min = 0.2):
  '''
  Simulated Annealing for heuristically solving the job shop 
  scheduling problem.

  params:
  jobs - an array of jobs whose order is being optimized
  T - starting temperature
  r - temperature decrease constant (aka how fast the temperature 
    should decrease between iterations). Must be positive and less than one
  L - number of positions swapped in each iteration
  T_min - the temperature at which a satisfactory ordering has been found

  returns - a heuristically-optimized ordering of jobs as a list
  '''

  frozen = False; #tracks if change occurs to the optimal makespan over the past few iterations
  s = jobs
  make_span_s = ms.calc_makespan(s, max_memory, max_cpus, tasks)

  #abort if calc_makespan returns -1
  if make_span_s == -1:
    raise Exception("Trying to process inviable list of jobs")

  temp_arr.append(T)
  makespan_arr.append(make_span_s)

  while not frozen:

    is_not_frozen = 0
    count = 0

    for i in range(L):
      
      #get random indices for shuffling elements in s
      i = ran.randint(0, len(jobs) - 1)
      j = ran.randint(0, len(jobs) - 1)

      #swap elements and calculate makespan of s'
      s_prime = swap(s, i, j)
      make_span_s_prime = ms.calc_makespan(s_prime, max_memory, max_cpus, tasks)
      
      #calculate and store change in makespan
      del_energy_state = make_span_s - make_span_s_prime

      if del_energy_state > 0:  # if s' makespan < s makespan, adopt s' as the 
        s = s_prime             #current minimum value
        make_span_s = make_span_s_prime
      elif del_energy_state < 0:
        p = math.exp(-del_energy_state / T)
        random = ran.uniform(0, 1)
        if random > p:     #if s' makespan > s makespan, adopt s' as the
          s = s_prime           #current minimum makespan upon a certain probablility
          make_span_s = make_span_s_prime #this prevents us from getting stuck in local minima
      
      is_not_frozen += del_energy_state
      count += 1
    
    if T <= T_min:
      frozen = True

    # check if any changes to min makespan occur in the iteration.
    #if none occur, we have found a makespan that is GOOD ENOUGH!
    if not is_not_frozen and count > 0:
      frozen = True

    T = r * T   #reduce temerpature using constant r
    temp_arr.append(T)
    makespan_arr.append(make_span_s)

  return s



if __name__ == "__main__":

  ### TESTING TESTING TESTING TESTING TESTING ###

  task_a = ms.PipelineTask(name="A", step=0, time_factor=4,
                          space_factor=1,
                          cpus=8)
  task_b = ms.PipelineTask(name="B", step=1, time_factor=6,
                        space_factor=1,
                        cpus=12)

  jobs1 = [25, 15, 10, 5, 33, 8, 22, 9, 18, 18, 28, 42, 37]
  #print(ms.calc_makespan(jobs1, 64, 16, [task_a, task_b]))
  #print(ms.calc_makespan(simulated_annealing(jobs1, 64, 16, [task_a, task_b]), 64, 16, [task_a, task_b]))


  jobs2 = [8, 9, 10, 12, 7, 15, 22, 19, 11, 37, 45, 44, 2, 11, 5, 6, 8, 27, 1, 19]
  out2 = simulated_annealing(jobs2, 64, 16, [task_a, task_b])
  print(out2)
  print(ms.calc_makespan(jobs2, 64, 16, [task_a, task_b]))
  print(ms.calc_makespan(simulated_annealing(jobs2, 64, 16, [task_a, task_b]), 64, 16, [task_a, task_b]))

  #create_plot(jobs2, 64, 16, [task_a, task_b])

  jobs3 = [25, 15, 10, 5]
  #print(ms.calc_makespan(jobs3, 64, 16, [task_a, task_b]))
  #out3 = simulated_annealing(jobs3, 64, 16, [task_a, task_b])
  #print(out3)
  #print(ms.calc_makespan(out3, 64, 16, [task_a, task_b]))


  task_a = ms.PipelineTask(name="A", step=0, time_factor=6,
                          space_factor=1,
                          cpus=4)
  task_b = ms.PipelineTask(name="B", step=1, time_factor=4,
                        space_factor=2,
                        cpus=6)
  task_c = ms.PipelineTask(name="C", step=2, time_factor=8,
                        space_factor=1,
                        cpus=8)

  jobs4 = [5, 8, 12, 11, 4, 11, 15, 14, 12, 7, 9, 2, 3, 3, 5, 8, 10]
  #out4 = simulated_annealing(jobs4, 32, 16, [task_a, task_b, task_c])
  #print(ms.calc_makespan(jobs4, 32, 16, [task_a, task_b, task_c]))
  #print(ms.calc_makespan(out4, 32, 16, [task_a, task_b, task_c]))