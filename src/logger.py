from datetime import datetime

def log(msg, line_break: bool = True, time: bool = True, b_print: bool = False):
    maybe_line_break = '\n' if line_break == True else ''
    maybe_time_str = datetime.utcnow().strftime('%y/%m/%d %H:%M:%S') +' - ' if time == True else '' 
    log_str = maybe_time_str + msg + maybe_line_break
    # print and write to log file
    if (b_print == True):
        print(log_str)
    with open('sub_log.txt', 'a') as the_file:
        the_file.write(log_str)
