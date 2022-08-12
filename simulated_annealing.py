import math
from scipy import constants
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


def simmulated_annealing(jobs, max_memory, max_cpus, tasks,  T=500, r=0.99, L=5):
  '''
  Simulated Annealing for heuristically solving the job shop 
  scheduling problem.

  params:
  jobs - an array of jobs whose order is being optimized
  T - starting temperature
  r - temperature decrease constant (aka how fast the temperature 
    should decrease between iterations)
  L - number of positions swapped in each iteration

  returns - a heuristically-optimized ordering of jobs as a list
  '''

  frozen = False; #tracks if change occurs to the optimal makespan over the past few iterations
  s = jobs
  make_span_s = ms.calc_makespan(s, max_memory, max_cpus, tasks)

  #abort if calc_makespan returns -1
  if make_span_s == -1:
    raise Exception("Trying to process inviable list of jobs")

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

      if del_energy_state < 0:  # if s' makespan < s makespan, adopt s' as the 
        s = s_prime             #current minimum value
        make_span_s = make_span_s_prime
      elif del_energy_state > 0:
        p = min(1, math.exp(-del_energy_state / constants.k * T))
        if ran.random >= p:     #if s' makespan > s makespan, adopt s' as the
          s = s_prime           #current minimum makespan upon a certain probablility
          make_span_s = make_span_s_prime #this prevents us from getting stuck in local minima
      
      is_not_frozen += del_energy_state
      count += 1

    # check if any changes to min makespan occur in the iteration.
    #if none occur, we have found a makespan that is GOOD ENOUGH!
    if not is_not_frozen and count > 0:
      frozen = True

    T = r * T   #reduce temerpature using constant r

  return s



if __name__ == "__main__":

  arr1 = [1, 2, 3]
  arr2 = arr1.copy()
  arr2[1] = 100

  print(arr1, arr2)