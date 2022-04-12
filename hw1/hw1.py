from hwfunctions import fun_factor, fun_inc
from dask import delayed



@delayed
def estimate_delayed_batch(start, end):
    ### delayed helper function which processes the incrementation for us
    increment = [fun_inc(i) for i in range(start, end)] # increment all values from start to end
    result = []

    increment_seg = [increment[i::4] for i in range(4)] # segment the increments into 4 (number of workers) 

    for segments in increment_seg:
        result.append(sum(segments)) # for each inc segment, append the sum to the results list

    return sum(result) # return delayed object of sum of results list


def delayed_increment(c, start, end):
    ### runs helper function 
    return estimate_delayed_batch(start, end) 


def delayed_factor(c, start, end):
    ### delayed function which factors values from start to end 
    result = []

    for i in range(start, end):
        x = delayed(fun_factor)(i) # for each value in range, run delayed function on value
        result.append(x) # add value to results list
    
    return delayed(sum)(result) # return delayed object with sum of results list



def estimate_future_batch(c, start, end):
    ### future helper function which processes the incrementation for us
    result = []
    for i in range(start, end):
        result.append(fun_inc(i))

    increment_seg = [result[i::4] for i in range(4)] # segment the increments into 4 (number of workers) 

    final = []

    for segments in increment_seg:
        final.append(sum(segments)) # for each inc segment, append the sum to the results list

    return c.submit(sum,final) # return future object of sum of results list


def future_increment(c, start, end):
    ### runs helper function 
    return estimate_future_batch(c, start, end)



def future_factor(c, start, end):
    ### future function which factors values from start to end 
    result = []

    for i in range(start, end):
        x = c.submit(fun_factor, i) # for each value in range, run future function on value
        result.append(x) # add value to results list
    
    return c.submit(sum, result) # return future object of sum of results list


